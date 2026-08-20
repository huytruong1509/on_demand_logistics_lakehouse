import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

from rag.dbt_parser import DbtManifestParser
from rag.qdrant_manager import QdrantSchemaManager

# Cấu hình logging chuẩn Production
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def run_indexing_job():
    logger.info("=== BẮT ĐẦU JOB ĐỒNG BỘ DBT METADATA SANG QDRANT ===")
    
    load_dotenv()

    # Sử dụng os.path.join hoặc Path chuẩn để tương thích đa nền tảng
    manifest_path = Path(os.getenv("DBT_MANIFEST_PATH", "dbt_transform/target/manifest.json")).resolve()
    catalog_path = Path(os.getenv("DBT_CATALOG_PATH", "dbt_transform/target/catalog.json")).resolve()

    logger.info(f"Đường dẫn manifest: {manifest_path}")
    logger.info(f"Đường dẫn catalog: {catalog_path}")

    if not manifest_path.exists() or not catalog_path.exists():
        error_msg = f"Thiếu file dbt artifacts.\n- Manifest: {manifest_path}\n- Catalog: {catalog_path}"
        logger.error(f"[FATAL] {error_msg}")
        raise FileNotFoundError(error_msg)

    try:
        # Bước A: Parse dbt JSON
        parser = DbtManifestParser(
            manifest_path=manifest_path, 
            catalog_path=catalog_path,
            allowed_schemas=["marts", "gold"] 
        )
        parsed_tables = parser.parse()
        
        if not parsed_tables:
            logger.warning("Job kết thúc sớm: Không tìm thấy bảng hợp lệ trong dbt artifacts.")
            return

        # Bước B: Khởi tạo Qdrant và Upsert theo Batch
        qdrant_manager = QdrantSchemaManager(collection_name="lakehouse_schema")
        qdrant_manager.init_collection()
        
        # Hàm upsert_tables bên qdrant_client_3.py sẽ tự dùng batch_size default là 20
        qdrant_manager.upsert_tables(parsed_tables)
        
        logger.info("=== JOB HOÀN TẤT THÀNH CÔNG ===")
        
    except Exception as e:
        logger.error(f"❌ Tiến trình Indexing thất bại. Lỗi: {str(e)}", exc_info=True)
        # Bỏ "raise e", chỉ dùng "raise" để bảo toàn bộ Call Stack Traceback nguyên thủy
        raise