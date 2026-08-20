"""
MinIO to Trino (Iceberg) Loader Module

Handles reading JSON from Landing Zone and inserting into Trino Iceberg tables.
"""

import logging
import json
from typing import Dict, Any, List
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class MinIOToTrinoLoader:
    """Manages data loading from MinIO Landing to Trino Bronze."""

    def __init__(self, s3_hook: S3Hook, trino_conn_id: str = "trino_default"):
        """
        Initialize Loader.
        
        Args:
            s3_hook: Airflow S3Hook for MinIO
            trino_conn_id: Tên connection của Trino khai báo trên giao diện Airflow
        """
        self.s3_hook = s3_hook
        self.trino_hook = TrinoHook(trino_conn_id=trino_conn_id)
        # Sử dụng Engine sinh ra từ Airflow Hook thay vì hardcode chuỗi kết nối
        # self.engine = self.trino_hook.get_sqlalchemy_engine()

    def load_json_to_bronze(self, bucket_name: str, s3_key: str, table_name: str, schema_name: str = 'bronze') -> Dict[str, Any]:
        """Đọc file JSON từ MinIO và Insert vào bảng Bronze."""
        
        logger.info(f"Bắt đầu nạp file s3://{bucket_name}/{s3_key} vào bảng {table_name}")
        
        try:
            # 1. Đọc nội dung file JSON từ MinIO
            file_content = self.s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
            data = json.loads(file_content)

            if not data:
                logger.warning(f"File {s3_key} rỗng, bỏ qua.")
                return {"status": "skipped", "rows_loaded": 0}

            # 2. Chuyển đổi thành DataFrame
            df = pd.DataFrame(data)
            total_rows = len(df)
            
            # 3. Tiền xử lý: Ép kiểu toàn bộ về chuỗi (VARCHAR) và đổi NaN thành None (NULL)
            df = df.astype(str)
            df = df.replace({'nan': None})

            # 4. Insert dữ liệu vào Trino
            logger.info("Đang ghi dữ liệu vào Trino Iceberg...")
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema_name,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"✓ Hoàn tất nạp {total_rows} dòng.")
            return {"status": "success", "rows_loaded": total_rows, "file_loaded": s3_key}

        except Exception as e:
            logger.error(f"Lỗi khi nạp file {s3_key}: {str(e)}")
            raise

    def validate_data_quality(self, table_name: str, rows_loaded_this_run: int, schema_name: str = 'bronze') -> Dict[str, Any]:
        """Chạy kiểm tra chất lượng dữ liệu và in thống kê."""
        
        logger.info("Bắt đầu kiểm tra chất lượng dữ liệu (Data Quality Check)...")
        
        # Câu query lấy tổng số dòng và đếm ID trùng lặp
        base_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT complaint_id) as unique_complaints,
                MIN(date_received) as earliest_date,
                MAX(date_received) as latest_date
            FROM lakehouse.{schema_name}.{table_name}
        """
        
        # Câu query lấy Top 5 công ty (phục vụ log)
        top_companies_query = f"""
            SELECT company, COUNT(*) as count 
            FROM lakehouse.{schema_name}.{table_name} 
            GROUP BY company 
            ORDER BY count DESC 
            LIMIT 5
        """
        
        try:
            with self.engine.connect() as conn:
                # Chạy query 1
                result = conn.execute(base_query).fetchone()
                total_rows, unique_complaints, earliest, latest = result
                
                # Chạy query 2
                top_companies_result = conn.execute(top_companies_query).fetchall()
                top_companies = [{"company": row[0], "count": row[1]} for row in top_companies_result]

            duplicate_count = total_rows - unique_complaints
            
            validation_results = {
                "total_rows_in_table": total_rows,
                "rows_loaded_this_run": rows_loaded_this_run,
                "earliest_date": earliest,
                "latest_date": latest,
                "duplicates": duplicate_count,
                "top_companies": top_companies
            }

            if duplicate_count > 0:
                logger.warning(f"CẢNH BÁO: Phát hiện {duplicate_count} dòng bị trùng lặp complaint_id!")
            else:
                logger.info("✓ Data Quality Check Passed: Không có ID trùng lặp.")
            
            # Gọi hàm in log thống kê y hệt project gốc
            self._log_statistics(validation_results)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy Data Quality Check: {e}")
            raise

    def _log_statistics(self, validation_results: Dict[str, Any]):
        """In bảng thống kê chi tiết ra màn hình log của Airflow."""
        logger.info("\n" + "=" * 80)
        logger.info("BẢNG THỐNG KÊ DỮ LIỆU BRONZE (LAKEHOUSE):")
        logger.info("-" * 80)
        logger.info(f"Tổng số dòng hiện có trong bảng : {validation_results['total_rows_in_table']:,}")
        logger.info(f"Số dòng vừa nạp trong lần chạy này: {validation_results['rows_loaded_this_run']:,}")

        if validation_results.get("earliest_date") and validation_results.get("latest_date"):
            logger.info(
                f"Phạm vi thời gian: Từ {validation_results['earliest_date']} đến {validation_results['latest_date']}"
            )

        logger.info("\nTop 5 công ty bị khiếu nại nhiều nhất:")
        for item in validation_results.get("top_companies", []):
            logger.info(f"  {item['company']}: {item['count']:,} khiếu nại")

        logger.info("=" * 80)
    

    def full_load_landing_to_bronze(self, target_catalog: str, target_schema: str, target_table: str, schema_file_path: str):
        """
        [SENIOR WAY] Pushdown Compute + Metadata-Driven
        Tự động sinh câu lệnh SQL dựa trên file schema.json. Có khả năng tự động
        xử lý các cột Audit (_ingest_time) mà không cần có trong dữ liệu thô.
        """
        try:
            # 1. Đọc cấu hình bảng từ file JSON Schema
            logger.info(f"Đọc định nghĩa cột từ: {schema_file_path}")
            with open(schema_file_path, "r") as f:
                schema_def = json.load(f)
            
            # 2. Trích xuất danh sách cột (Dynamic Columns)
            bronze_columns = schema_def.get("bronze_layer", {}).get("columns", {})
            if not bronze_columns:
                raise ValueError("Không tìm thấy định nghĩa columns cho bronze_layer trong schema.json")
            
            # Danh sách cột cho câu lệnh INSERT (Gồm tất cả các cột)
            insert_column_names = list(bronze_columns.keys())
            insert_columns_sql = ",\n                    ".join(insert_column_names)
            
            # [LƯỚI LỌC THÔNG MINH] Danh sách cột cho câu lệnh SELECT
            select_column_names = []
            for col in insert_column_names:
                if col.lower() == "_ingest_time":
                    select_column_names.append("CURRENT_TIMESTAMP AS _ingest_time")
                elif col.lower() == "_run_id":
                    select_column_names.append("CAST(uuid() AS VARCHAR) AS _run_id")
                elif col.lower() == "_source_system":
                    select_column_names.append("'CFPB_API' AS _source_system")
                else:
                    select_column_names.append(col)
                    
            select_columns_sql = ",\n                    ".join(select_column_names)

            # 3. Xóa dữ liệu cũ
            logger.info(f"🗑️ Đang xóa dữ liệu cũ trong {target_schema}.{target_table} (FULL LOAD)...")
            delete_sql = f"DELETE FROM {target_catalog}.{target_schema}.{target_table}"
            self.trino_hook.run(delete_sql)
            
            # 4. Lắp ráp và chạy câu lệnh SQL động
            logger.info("🚀 Đang nạp dữ liệu từ MinIO vào Iceberg (Metadata-Driven)...")
            insert_sql = f"""
                INSERT INTO {target_catalog}.{target_schema}.{target_table} (
                    {insert_columns_sql}
                )
                SELECT 
                    {select_columns_sql}
                FROM minio_landing.default.raw_json_complaints
            """
            
            logger.debug(f"Executing SQL:\n{insert_sql}")
            self.trino_hook.run(insert_sql)
            
            # 5. Kiểm tra kết quả
            count_sql = f"SELECT COUNT(*) FROM {target_catalog}.{target_schema}.{target_table}"
            records = self.trino_hook.get_first(count_sql)
            rows_loaded = records[0] if records else 0
            
            logger.info(f"✓ Pushdown Compute hoàn tất! Đã load {rows_loaded:,} dòng.")
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Lỗi khi Load dữ liệu vào Trino: {e}")
            raise
        
    def incremental_insert_to_bronze(self, target_catalog: str, target_schema: str, target_table: str, schema_file_path: str, run_date: str, run_id: str):
        """
        [SENIOR WAY] Nạp dữ liệu Incremental bằng lệnh INSERT, có kiểm tra Schema Drift.
        """
        try:
            # 1. Đọc Schema JSON (Bản vẽ)
            with open(schema_file_path, "r") as f:
                schema_def = json.load(f)
            expected_columns = schema_def.get("bronze_layer", {}).get("columns", {})
            expected_keys = set(col.lower() for col in expected_columns.keys())

            # 2. Kiểm tra Database Schema thực tế (EDGE CASE 3 KHẮC PHỤC)
            logger.info("🔍 Đang kiểm tra đối soát Schema (Reconciliation Check)...")
            show_cols_sql = f"SHOW COLUMNS FROM {target_catalog}.{target_schema}.{target_table}"
            actual_cols_records = self.trino_hook.get_records(show_cols_sql)
            actual_keys = set(row[0].lower() for row in actual_cols_records)

            missing_in_db = expected_keys - actual_keys
            if missing_in_db:
                error_msg = f"CRITICAL: Lệch pha Schema! schema.json có các cột {missing_in_db} nhưng Database chưa có. Vui lòng ALTER TABLE!"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # 3. Tạo/Replace External Table trỏ vào phân vùng MinIO
            ext_table = f"ext_landing_complaints_{run_date.replace('-', '')}"
            ext_location = f"s3://lakehouse/landing/consumer_complaints/run_date={run_date}/"
            
            # Trích xuất định nghĩa cột cho external table từ schema_def (landing_layer)
            landing_cols_def = schema_def.get("landing_layer", {}).get("columns", {})
            ext_cols_sql = ",\n                ".join([f"{k} {v}" for k, v in landing_cols_def.items()])

            logger.info(f"Tạo External Table tạm thời: {ext_table}")
            self.trino_hook.run(f"DROP TABLE IF EXISTS minio_landing.default.{ext_table}")
            create_ext_sql = f"""
                CREATE TABLE minio_landing.default.{ext_table} (
                    {ext_cols_sql}
                ) WITH (
                    format = 'JSON',
                    external_location = '{ext_location}'
                )
            """
            self.trino_hook.run(create_ext_sql)

            # 4. Sinh lệnh INSERT động
            insert_column_names = list(expected_columns.keys())
            insert_columns_sql = ",\n                ".join(insert_column_names)
            
            select_column_names = []
            for col in insert_column_names:
                col_lower = col.lower()
                if col_lower == "_ingest_time":
                    select_column_names.append("CURRENT_TIMESTAMP AS _ingest_time")
                elif col_lower == "_run_id":
                    select_column_names.append(f"'{run_id}' AS _run_id")
                elif col_lower == "_source_system":
                    select_column_names.append("'CFPB_API' AS _source_system")
                else:
                    select_column_names.append(col)
                    
            select_columns_sql = ",\n                ".join(select_column_names)

            logger.info(f"🚀 Đang INSERT dữ liệu vào Bronze Zone...")
            insert_sql = f"""
                INSERT INTO {target_catalog}.{target_schema}.{target_table} (
                    {insert_columns_sql}
                )
                SELECT 
                    {select_columns_sql}
                FROM minio_landing.default.{ext_table}
            """
            self.trino_hook.run(insert_sql)

            # 5. [BỔ SUNG] Đếm số dòng thực tế đã vào bảng Bronze
            logger.info("🔍 Đang đối soát số lượng record đã nạp...")
            target_count_sql = f"SELECT COUNT(*) FROM {target_catalog}.{target_schema}.{target_table} WHERE _run_id = '{run_id}'"
            target_records = self.trino_hook.get_first(target_count_sql)
            actual_inserted_count = target_records[0] if target_records else 0

            # 6. Dọn dẹp External Table
            self.trino_hook.run(f"DROP TABLE IF EXISTS minio_landing.default.{ext_table}")
            logger.info(f"✓ Hoàn tất Incremental Load! Đã nạp thành công {actual_inserted_count} dòng.")

            return actual_inserted_count # Trả về số dòng để Airflow đối soát

        except Exception as e:
            logger.error(f"Lỗi khi Incremental Load: {e}")
            raise