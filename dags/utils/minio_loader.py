"""
Generic MinIO Data Loader Module (Landing Zone)

Xử lý việc upload các file JSON thô lên MinIO và dọn dẹp file cũ.
Dùng chung được cho nhiều pipeline (Logistics, Complaints, v.v.)
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

class MinIOLoader:
    """Quản lý các thao tác upload và dọn dẹp file trên MinIO (S3-compatible)."""

    def __init__(self, s3_hook: S3Hook, bucket_name: str, prefix: str):
        """
        Khởi tạo MinIOLoader.

        Args:
            s3_hook: Airflow S3Hook (Sẽ được cấu hình trỏ tới MinIO)
            bucket_name: Tên bucket trên MinIO (VD: 'lakehouse')
            prefix: Thư mục chứa file (Landing Zone)
        """
        self.s3_hook = s3_hook
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip('/')

    def cleanup_old_files(self, entity_name: str, current_key: str):
        """
        Xóa các file cũ của một entity (VD: city_id), CHỈ giữ lại file mới nhất vừa upload.
        """
        try:
            keys = self.s3_hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=f"{self.prefix}/",
            )

            if not keys:
                return

            # Pattern linh hoạt hơn, chỉ cần chứa entity_name và kết thúc bằng .json
            pattern = rf"{self.prefix}/\d{{8}}_\d{{6}}_{re.escape(entity_name)}.*\.json"

            files_to_delete = []
            for key in keys:
                if re.match(pattern, key) and key != current_key:
                    files_to_delete.append(key)

            if files_to_delete:
                logger.info(f"🗑️ Đang dọn dẹp {len(files_to_delete)} file rác cũ của {entity_name}")
                for key in files_to_delete:
                    self.s3_hook.delete_objects(bucket=self.bucket_name, keys=[key])
                    logger.info(f"   Đã xóa: {key}")

        except Exception as e:
            logger.warning(f"Lỗi khi dọn dẹp file cũ cho {entity_name}: {e}")

    def list_files(self) -> List[str]:
        """Liệt kê toàn bộ file trong prefix."""
        try:
            keys = self.s3_hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=f"{self.prefix}/",
            )
            return keys or []
        except Exception as e:
            logger.error(f"Lỗi khi list files: {e}")
            return []
    
    def purge_landing_zone(self):
        """
        [SENIOR WAY] Xóa sạch toàn bộ file cũ trong Landing Zone trước khi chạy Full Load.
        """
        try:
            keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=f"{self.prefix}/")
            if keys:
                logger.info(f"🧹 Đang xóa sạch {len(keys)} file cũ trong Landing Zone ({self.prefix}/)...")
                self.s3_hook.delete_objects(bucket=self.bucket_name, keys=keys)
                logger.info("✓ Landing Zone đã hoàn toàn sạch sẽ!")
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp Landing Zone: {e}")
            raise

    def upload_files(self, source_files: Dict[str, str]) -> List[Dict[str, any]]:
        """Upload file JSON lên MinIO (Dùng cho thư mục gốc Landing)."""
        uploaded_files = []
        for entity_name, local_path in source_files.items():
            try:
                file_name = Path(local_path).name
                s3_key = f"{self.prefix}/{file_name}"
                
                logger.info(f"Đang upload {file_name} lên s3://{self.bucket_name}/{s3_key}")
                self.s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=self.bucket_name,
                    replace=True
                )
                uploaded_files.append({"entity": entity_name, "s3_key": s3_key})
            except Exception as e:
                logger.error(f"Lỗi khi upload file cho {entity_name}: {e}")
                raise
                
        return uploaded_files
    
    def purge_landing_partition(self, run_date: str):
        """
        [INCREMENTAL WAY] Xóa sạch file trong phân vùng của phiên chạy hiện tại.
        """
        partition_prefix = f"{self.prefix}/run_date={run_date}"
        try:
            keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=f"{partition_prefix}/")
            if keys:
                logger.info(f"🧹 Đang xóa {len(keys)} file cũ trong phân vùng {partition_prefix}...")
                self.s3_hook.delete_objects(bucket=self.bucket_name, keys=keys)
                logger.info("✓ Phân vùng đã sạch sẽ, sẵn sàng nhận data mới!")
            else:
                logger.info(f"ℹ️ Phân vùng {partition_prefix} đang trống.")
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp phân vùng {partition_prefix}: {e}")
            raise

    def upload_incremental_files(self, source_files: Dict[str, str], run_date: str) -> List[Dict[str, any]]:
        """Upload file JSON lên MinIO vào đúng thư mục phân vùng ngày."""
        uploaded_files = []
        partition_prefix = f"{self.prefix}/run_date={run_date}"
        
        for entity_name, local_path in source_files.items():
            try:
                file_name = Path(local_path).name
                s3_key = f"{partition_prefix}/{file_name}"
                
                logger.info(f"📤 Đang upload {file_name} lên s3://{self.bucket_name}/{s3_key}")
                self.s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=self.bucket_name,
                    replace=True
                )
                uploaded_files.append({"entity": entity_name, "s3_key": s3_key})
            except Exception as e:
                logger.error(f"Lỗi khi upload file cho {entity_name}: {e}")
                raise
                
        return uploaded_files