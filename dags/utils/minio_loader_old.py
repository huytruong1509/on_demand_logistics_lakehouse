"""
MinIO Data Loader Module (Landing Zone)

Xử lý việc upload các file JSON thô lên MinIO và dọn dẹp file cũ.
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

    def __init__(self, s3_hook: S3Hook, bucket_name: str, prefix: str = "landing/consumer_complaints"):
        """
        Khởi tạo MinIOLoader.

        Args:
            s3_hook: Airflow S3Hook (Sẽ được cấu hình trỏ tới MinIO)
            bucket_name: Tên bucket trên MinIO (VD: 'lakehouse')
            prefix: Thư mục chứa file (Landing Zone)
        """
        self.s3_hook = s3_hook
        self.bucket_name = bucket_name
        self.prefix = prefix

    # def upload_files(self, company_files: Dict[str, str]) -> List[Dict[str, any]]:
    #     """
    #     Upload các file JSON lên MinIO, đính kèm timestamp vào tên file và xóa file cũ.

    #     Args:
    #         company_files: Dictionary mapping tên công ty với đường dẫn file JSON dưới local (máy ảo Airflow)

    #     Returns:
    #         List các dictionary chứa thông tin file đã upload (company, s3_key, size_mb)
    #     """
    #     if not company_files:
    #         logger.warning("Không có file nào để upload lên MinIO")
    #         return []

    #     logger.info(f"Đang upload {len(company_files)} files lên bucket: {self.bucket_name}")

    #     uploaded_files = []
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    #     for company_name, file_path in company_files.items():
    #         try:
    #             # Làm sạch tên công ty để tạo tên file chuẩn (xóa khoảng trắng, in thường)
    #             sanitized_name = company_name.replace(" ", "_").lower()

    #             # ĐÃ SỬA: Đổi đuôi .csv thành .json cho đúng chuẩn Raw Data
    #             s3_key = f"{self.prefix}/{timestamp}_{sanitized_name}_complaints.json"

    #             # Lấy dung lượng file
    #             file_size = Path(file_path).stat().st_size
    #             file_size_mb = file_size / (1024 * 1024)

    #             logger.info(f"📁 Đang upload {sanitized_name}_complaints.json ({file_size_mb:.2f} MB)")

    #             # Upload lên MinIO sử dụng hàm load_file của S3Hook
    #             self.s3_hook.load_file(
    #                 filename=file_path,
    #                 key=s3_key,
    #                 bucket_name=self.bucket_name,
    #                 replace=True,
    #             )

    #             logger.info(f"✓ Đã upload thành công lên s3://{self.bucket_name}/{s3_key}")

    #             uploaded_files.append(
    #                 {
    #                     "company": company_name,
    #                     "s3_key": s3_key,
    #                     "size_mb": file_size_mb,
    #                 }
    #             )

    #             # Dọn dẹp các file JSON của ngày hôm trước để tiết kiệm dung lượng
    #             self.cleanup_old_files(sanitized_name, s3_key)

    #         except Exception as e:
    #             logger.error(f"Lỗi khi upload data cho {company_name}: {e}")
    #             raise

    #     logger.info(f"✓ Hoàn tất upload {len(uploaded_files)} files lên Landing Zone")
    #     return uploaded_files

    def cleanup_old_files(self, company_name: str, current_key: str):
        """
        Xóa các file cũ của một công ty cụ thể, CHỈ giữ lại file mới nhất vừa upload.
        """
        try:
            # Liệt kê toàn bộ file trong thư mục landing/
            keys = self.s3_hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=f"{self.prefix}/",
            )

            if not keys:
                return

            # ĐÃ SỬA: Pattern Regex giờ sẽ tìm kiếm file đuôi .json thay vì .csv
            pattern = rf"{self.prefix}/\d{{8}}_\d{{6}}_{re.escape(company_name)}_complaints\.json"

            files_to_delete = []
            for key in keys:
                if re.match(pattern, key) and key != current_key:
                    files_to_delete.append(key)

            if files_to_delete:
                logger.info(f"🗑️ Đang dọn dẹp {len(files_to_delete)} file rác cũ của {company_name}")
                for key in files_to_delete:
                    self.s3_hook.delete_objects(bucket=self.bucket_name, keys=[key])
                    logger.info(f"   Đã xóa: {key}")

        except Exception as e:
            logger.warning(f"Lỗi khi dọn dẹp file cũ cho {company_name}: {e}")
            # Lỗi dọn dẹp thì chỉ báo warning chứ không làm sập pipeline (pass soft fail)

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
        Đảm bảo tính Idempotent (chạy lại bao nhiêu lần kết quả vẫn đúng).
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

    def upload_files(self, company_files: Dict[str, str]) -> List[Dict[str, any]]:
        """Upload file JSON lên MinIO."""
        uploaded_files = []
        for company_name, local_path in company_files.items():
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
                uploaded_files.append({"company": company_name, "s3_key": s3_key})
            except Exception as e:
                logger.error(f"Lỗi khi upload file cho {company_name}: {e}")
                raise
                
        return uploaded_files
    
    def purge_landing_partition(self, run_date: str):
        """
        [INCREMENTAL WAY] Xóa sạch file trong phân vùng của phiên chạy hiện tại.
        Đảm bảo Idempotency cho Incremental Load.
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

    def upload_incremental_files(self, company_files: Dict[str, str], run_date: str) -> List[Dict[str, any]]:
        """Upload file JSON lên MinIO vào đúng thư mục phân vùng."""
        uploaded_files = []
        partition_prefix = f"{self.prefix}/run_date={run_date}"
        
        for company_name, local_path in company_files.items():
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
                uploaded_files.append({"company": company_name, "s3_key": s3_key})
            except Exception as e:
                logger.error(f"Lỗi khi upload file cho {company_name}: {e}")
                raise
                
        return uploaded_files