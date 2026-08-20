import os
import logging
import uuid
from typing import List, Iterator, Optional, Any
from dataclasses import dataclass, field

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, OptimizersConfigDiff
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.api_core.exceptions import ResourceExhausted, InternalServerError, ServiceUnavailable

from .schema_models import TableMetadata

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class QdrantGeminiConfig:
    """
    Quản lý cấu hình Immutable (Thread-safe).
    Bổ sung cấu hình gRPC và Timeout cho Production.
    """
    qdrant_host: str = field(default_factory=lambda: os.getenv("QDRANT_HOST", "qdrant"))
    qdrant_port: int = field(default_factory=lambda: int(os.getenv("QDRANT_PORT", "6333")))
    qdrant_grpc_port: int = field(default_factory=lambda: int(os.getenv("QDRANT_GRPC_PORT", "6334")))
    qdrant_timeout: float = field(default_factory=lambda: float(os.getenv("QDRANT_TIMEOUT", "10.0")))
    
    embed_model: str = field(default_factory=lambda: os.getenv("GEMINI_EMBEDDING_MODEL", "models/gemini-embedding-2"))
    vector_size: int = 3072 
    distance_metric: Distance = Distance.COSINE 

    @property
    def api_key(self) -> str:
        key = os.getenv("GEMINI_API_KEY")
        if not key:
            raise ValueError("[CRITICAL] Missing GEMINI_API_KEY environment variable.")
        return key


class QdrantSchemaManager:
    """
    Trình quản lý Qdrant Vector Database cho Schema/Metadata.
    """
    def __init__(self, collection_name: str = "lakehouse_schema", config: Optional[QdrantGeminiConfig] = None):
        self.collection_name = collection_name
        self.config = config or QdrantGeminiConfig()
        
        # [CẢI TIẾN] Bật prefer_grpc=True để tối ưu hóa hiệu năng Network I/O
        # Cấu hình timeout rõ ràng chặn việc treo request
        self.client = QdrantClient(
            host=self.config.qdrant_host, 
            port=self.config.qdrant_port,
            grpc_port=self.config.qdrant_grpc_port,
            prefer_grpc=True,
            timeout=self.config.qdrant_timeout
        )

        self.embeddings = GoogleGenerativeAIEmbeddings(
            model=self.config.embed_model,
            google_api_key=self.config.api_key
        )

    def init_collection(self) -> None:
        if not self.client.collection_exists(self.collection_name):
            logger.info(f"Khởi tạo Collection '{self.collection_name}' | Model: {self.config.embed_model}")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.config.vector_size,
                    distance=self.config.distance_metric
                ),
                # [CẢI TIẾN] Điều chỉnh Optimizer để index hiệu quả hơn khi bulk upload
                optimizers_config=OptimizersConfigDiff(default_segment_number=2)
            )
        else:
            logger.info(f"Collection '{self.collection_name}' đã sẵn sàng.")

    @staticmethod
    def _generate_deterministic_id(schema_name: str, table_name: str) -> str:
        namespace_string = f"{schema_name}.{table_name}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, namespace_string))

    @staticmethod
    def _format_for_asymmetric_retrieval(table: TableMetadata) -> str:
        title = f"{table.schema_name}.{table.table_name}"
        content = table.to_document_string().strip() or "none"
        return f"title: {title} | text: {content}"

    @staticmethod
    def _chunk_generator(data: List[Any], batch_size: int) -> Iterator[List[Any]]:
        """Đã fix cú pháp List[Any]."""
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    @retry(
        retry=retry_if_exception_type((ResourceExhausted, InternalServerError, ServiceUnavailable)),
        stop=stop_after_attempt(5), 
        wait=wait_exponential(multiplier=2, min=4, max=60),
        reraise=True
    )
    def _call_embedding_api_safely(self, texts: List[str]) -> List[List[float]]:
        return self.embeddings.embed_documents(texts)

    def upsert_tables(self, parsed_tables: List[TableMetadata], batch_size: int = 50) -> None:
        """
        Batching size có thể nâng lên 50 hoặc 100 vì gRPC xử lý payload lớn tốt hơn REST rất nhiều.
        """
        if not parsed_tables:
            logger.warning("Danh sách bảng trống. Bỏ qua tác vụ Upsert.")
            return

        logger.info(f"Bắt đầu Upsert {len(parsed_tables)} tables. Batch size: {batch_size}")

        for batch_idx, batch in enumerate(self._chunk_generator(parsed_tables, batch_size)):
            formatted_texts = [self._format_for_asymmetric_retrieval(table) for table in batch]

            try:
                vectors = self._call_embedding_api_safely(formatted_texts)
            except Exception as e:
                logger.error(f"[CRITICAL] Lỗi Embedding API tại batch số {batch_idx + 1}: {e}")
                raise

            points = [
                PointStruct(
                    id=self._generate_deterministic_id(table.schema_name, table.table_name),
                    vector=vector,
                    payload={
                        "schema_name": table.schema_name,
                        "table_name": table.table_name,
                        "description": table.description,
                        "raw_context": table.to_document_string() 
                    }
                )
                for table, vector in zip(batch, vectors)
            ]

            # [CẢI TIẾN CỐT LÕI] Thêm wait=True để đảm bảo dữ liệu available cho Search ngay lập tức sau pipeline
            self.client.upsert(
                collection_name=self.collection_name,
                points=points,
                wait=True 
            )
            
            logger.info(f"Đã xử lý xong batch {batch_idx + 1} ({len(batch)} records).")