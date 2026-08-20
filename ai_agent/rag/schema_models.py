from pydantic import BaseModel, Field
from typing import List, Optional

class ColumnMetadata(BaseModel):
    name: str
    data_type: str = "UNKNOWN"
    description: Optional[str] = ""

class TableMetadata(BaseModel):
    table_name: str
    schema_name: str
    description: Optional[str] = ""
    columns: List[ColumnMetadata] = Field(default_factory=list)

    def to_document_string(self) -> str:
        """
        Hàm này cực kỳ quan trọng cho RAG. 
        Định dạng chuỗi càng chuẩn, LLM càng dễ hiểu và sinh SQL chính xác.
        """
        # TỐI ƯU: Nối sẵn thành FQN để LLM bốc nguyên cụm này nhét vào câu SELECT
        fqn = f"lakehouse.{self.schema_name}.{self.table_name}"
        
        # Format danh sách cột rõ ràng
        col_strings = []
        for col in self.columns:
            desc = f" - {col.description}" if col.description else ""
            col_strings.append(f"- {col.name} ({col.data_type}){desc}")
            
        cols_formatted = "\n".join(col_strings)

        # Trả về string document chuẩn mực
        return (
            f"Table FQN: {fqn}\n"
            f"Table Name: {self.table_name}\n"
            f"Description: {self.description}\n"
            f"Columns:\n{cols_formatted}"
        )