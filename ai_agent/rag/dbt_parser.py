import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Set

from .schema_models import TableMetadata, ColumnMetadata

logger = logging.getLogger(__name__)

class DbtManifestParser:
    """Engine đọc, bóc tách và hợp nhất dữ liệu từ dbt artifacts (Production Ready)."""
    
    def __init__(
        self, 
        manifest_path: str | Path, 
        catalog_path: str | Path, 
        allowed_schemas: Optional[List[str]] = None
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.catalog_path = Path(catalog_path)
        # Sử dụng set comprehension chuẩn type hint
        self.allowed_schemas: Set[str] = {s.lower() for s in (allowed_schemas or ["marts", "gold"])}

    def _load_json(self, file_path: Path) -> Dict[str, Any]:
        """Đọc JSON an toàn với exception handling chi tiết."""
        if not file_path.is_file():
            logger.error(f"[CRITICAL] Artifact không tồn tại hoặc không phải là file: {file_path}")
            raise FileNotFoundError(f"Missing artifact: {file_path}")
            
        try:
            with file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError(f"Root JSON object phải là Dictionary. File: {file_path}")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"[CRITICAL] Corrupted JSON tại {file_path}. Chi tiết: {e}")
            raise ValueError(f"Invalid JSON format in {file_path}") from e

    def parse(self) -> List[TableMetadata]:
        logger.info(f"Bắt đầu parse dbt artifacts. Allowed schemas: {self.allowed_schemas}")
        
        manifest = self._load_json(self.manifest_path)
        catalog = self._load_json(self.catalog_path)

        manifest_nodes = manifest.get("nodes", {})
        catalog_nodes = catalog.get("nodes", {})

        if not manifest_nodes:
            logger.warning("Manifest không chứa key 'nodes'. Dừng pipeline.")
            return []

        parsed_tables: List[TableMetadata] = []

        for unique_id, node_data in manifest_nodes.items():
            if node_data.get("resource_type") != "model":
                continue
                
            schema_name = str(node_data.get("schema", "")).lower()
            if schema_name not in self.allowed_schemas:
                continue

            table_name = node_data.get("name")
            if not table_name:
                logger.debug(f"Skip node {unique_id} do thiếu tên bảng.")
                continue

            # TỐI ƯU: Normalize catalog columns dictionary keys sang lowercase một lần duy nhất
            raw_catalog_cols = catalog_nodes.get(unique_id, {}).get("columns", {})
            catalog_columns_lower = {k.lower(): v for k, v in raw_catalog_cols.items()}
            
            parsed_columns: List[ColumnMetadata] = []
            
            for col_name, col_data in node_data.get("columns", {}).items():
                # Lookup O(1) an toàn và nhanh chóng
                cat_col_data = catalog_columns_lower.get(col_name.lower(), {})
                
                parsed_columns.append(
                    ColumnMetadata(
                        name=col_name,
                        data_type=cat_col_data.get("type", "UNKNOWN"),
                        description=col_data.get("description", "")
                    )
                )

            parsed_tables.append(
                TableMetadata(
                    table_name=table_name,
                    schema_name=schema_name,
                    description=node_data.get("description", ""),
                    columns=parsed_columns
                )
            )

        # Giải phóng bộ nhớ cho các dict khổng lồ
        del manifest, catalog 

        logger.info(f"Hoàn tất! Bóc tách thành công {len(parsed_tables)} bảng.")
        return parsed_tables