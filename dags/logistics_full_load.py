"""
Logistics Orders Full Load DAG
Nhiệm vụ: Pull toàn bộ dữ liệu lịch sử từ Orders API tập trung trong 1 task,
sau đó nạp vào bảng Bronze trên Iceberg thông qua MinIO & DuckDB.
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict

from airflow.decorators import dag, task
from pendulum import datetime as pendulum_datetime
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.hooks.base import BaseHook

from config import (
    API_BASE_URL, MINIO_CONN_ID, LANDING_BUCKET, LANDING_PREFIX, 
    TRINO_CONN_ID, TRINO_CATALOG, TRINO_SCHEMA, TARGET_TABLE,
    NESSIE_CONN_ID
)

from utils.orders_api_client import OrdersAPIClient
from utils.minio_loader import MinIOLoader
from utils.duckdb_api_to_bronze import DuckDBIcebergLoader

SCHEMA_FILE_PATH = Path(__file__).parent / "schemas" / "logistics_schema.json"

@dag(
    dag_id="logistics_orders_full_load",
    start_date=pendulum_datetime(2026, 1, 1, tz="UTC"),
    schedule_interval=None, 
    catchup=False, 
    max_active_runs=1,
    tags=["logistics", "full-load", "lakehouse", "batch"],
)
def logistics_orders_full_load():

    @task
    def prepare_and_clean_target(**context) -> str:
        """Dọn dẹp môi trường cho luồng Full Load."""
        logger = logging.getLogger(__name__)
        logical_date = context['logical_date']
        run_date = logical_date.strftime('%Y-%m-%d')
        
        try:
            s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
            loader = MinIOLoader(s3_hook=s3_hook, bucket_name=LANDING_BUCKET, prefix=LANDING_PREFIX)
            loader.purge_landing_zone() 
            logger.info("✅ [LANDING] Đã dọn dẹp TOÀN BỘ thư mục Landing cho luồng Full Load.")
        except Exception as e:
            logger.error(f"❌ [LANDING] Lỗi nghiêm trọng khi clear MinIO: {e}")
            raise AirflowException(f"Dừng DAG vì không thể clear Landing Zone: {e}")

        try:
            trino_hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
            truncate_query = f"TRUNCATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}"
            logger.info(f"🧹 [BRONZE] Đang chạy TRUNCATE cho bảng {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}...")
            
            trino_hook.run(truncate_query)
            logger.info("✅ [BRONZE] Đã TRUNCATE thành công. Bảng đã sạch sẽ, sẵn sàng nạp Full Load.")
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                logger.warning(f"⚠️ [BRONZE] Bỏ qua TRUNCATE vì bảng/schema chưa tồn tại (Khởi tạo Day 1): {e}")
            else:
                logger.error(f"❌ [BRONZE] Lỗi TRUNCATE bảng Iceberg: {e}")
                raise AirflowException(f"Không thể dọn dẹp bảng Iceberg: {e}")

        return run_date

    @task
    def extract_and_upload_full(run_date: str) -> int:
        """Kéo toàn bộ dữ liệu API tập trung, stream ra file đĩa và nạp lên MinIO."""
        logger = logging.getLogger(__name__)
        
        with open(SCHEMA_FILE_PATH, "r") as f:
            schema_def = json.load(f)
            
        all_columns = schema_def.get("landing_layer", {}).get("columns", {}).keys()
        SYSTEM_EXCLUDE_COLS = {"_rescued_data", "_ingest_time", "_run_id", "_source_system"}
        expected_keys = {col for col in all_columns if col not in SYSTEM_EXCLUDE_COLS}

        api_client = OrdersAPIClient(base_url=API_BASE_URL)
        temp_dir = tempfile.gettempdir()
        file_path = Path(temp_dir) / f"full_{run_date}_all_orders.json"
        
        total_extracted_rows = 0

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                for chunk in api_client.get_batch_orders():
                    for record in chunk:
                        processed_record = {}
                        unknown_fields = {}
                        
                        for key, value in record.items():
                            if value is None:
                                continue
                            
                            if key in expected_keys:
                                if isinstance(value, (dict, list)):
                                    processed_record[key] = json.dumps(value, ensure_ascii=False)
                                else:
                                    processed_record[key] = str(value)
                            else:
                                unknown_fields[key] = value

                        if unknown_fields:
                            processed_record["_rescued_data"] = json.dumps(unknown_fields, ensure_ascii=False)

                        f.write(json.dumps(processed_record, ensure_ascii=False) + "\n")
                        total_extracted_rows += 1 

            if total_extracted_rows > 0:
                s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
                loader = MinIOLoader(s3_hook=s3_hook, bucket_name=LANDING_BUCKET, prefix=LANDING_PREFIX)
                loader.upload_incremental_files({"ALL": str(file_path)}, run_date)

        except Exception as e:
            logger.error(f"Lỗi kéo toàn bộ dữ liệu Full Load: {e}")
            raise AirflowException(f"Full Load thất bại: {e}")
        finally:
            if os.path.exists(file_path): 
                os.remove(file_path)
            api_client.close()

        return total_extracted_rows

    @task
    def full_load_stream_to_bronze(run_date: str, total_extracted: int, **context) -> Dict[str, Any]:
        logger = logging.getLogger(__name__)
        run_id = context['run_id']
        
        if total_extracted == 0:
            return {"inserted_run_id": run_id, "total_extracted_rows": 0, "total_inserted_rows": 0}

        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
        nessie_conn = BaseHook.get_connection(NESSIE_CONN_ID)
        nessie_uri = f"http://{nessie_conn.host}:{nessie_conn.port}/iceberg"

        with open(SCHEMA_FILE_PATH, "r") as f:
            schema_def = json.load(f)
        bronze_columns = schema_def["bronze_layer"]["columns"]

        with DuckDBIcebergLoader(
            s3_endpoint=minio_conn.extra_dejson.get("endpoint_url", "http://minio:9000"),
            s3_access_key=minio_conn.login,
            s3_secret_key=minio_conn.password,
            nessie_uri=nessie_uri
        ) as loader:
            inserted_rows = loader.load_stream(
                bucket=LANDING_BUCKET,
                prefix=LANDING_PREFIX,
                target_schema=TRINO_SCHEMA,
                target_table=TARGET_TABLE,
                run_id=run_id,
                run_date=run_date,
                bronze_columns=bronze_columns,
                source_system="ORDERS_API_BATCH",
                chunk_size=250000 
            )
            
            if inserted_rows == 0 and total_extracted > 0:
                raise AirflowException("Streaming bị lỗi ngầm. Không có dòng nào được nạp vào Iceberg.")
        
        return {
            "inserted_run_id": run_id,
            "total_extracted_rows": total_extracted,
            "total_inserted_rows": inserted_rows 
        }

    @task
    def audit_rescued_data_alert(insert_data: Dict[str, Any], **context) -> Dict[str, Any]:
        if insert_data["total_inserted_rows"] == 0:
            insert_data["total_rescued_rows"] = 0
            return insert_data

        inserted_run_id = insert_data["inserted_run_id"]
        trino_hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        logger = logging.getLogger(__name__)
        
        audit_sql = f"""
            SELECT COUNT(*) 
            FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE} 
            WHERE _run_id = '{inserted_run_id}' 
              AND _rescued_data IS NOT NULL
        """
        try:
            records = trino_hook.get_first(audit_sql)
            rescued_count = records[0] if records else 0
        except Exception as e:
            logger.warning(f"Bỏ qua Audit do Trino chưa sync kịp hoặc bảng lỗi: {e}")
            rescued_count = 0
        
        if rescued_count > 0:
            logger.error(f"🚨 [CẢNH BÁO SCHEMA DRIFT] Phát hiện {rescued_count} dòng chứa dữ liệu rác/lệch cấu trúc.")
        else:
            logger.info("✅ Dữ liệu an toàn. Không phát hiện Rescued Data.")

        insert_data["total_rescued_rows"] = rescued_count
        return insert_data

    @task
    def reconciliation_summary(audit_data: Dict[str, Any], **context):
        logger = logging.getLogger(__name__)
        extracted = audit_data.get("total_extracted_rows", 0)
        inserted = audit_data.get("total_inserted_rows", 0)
        rescued = audit_data.get("total_rescued_rows", 0)
        run_id = audit_data.get("inserted_run_id")

        summary_msg = f"""
        📊 BÁO CÁO FULL LOAD ĐỐI SOÁT
        - Run ID: {run_id}
        - Dữ liệu API (Extracted): {extracted:,}
        - Nạp Bronze  (Inserted) : {inserted:,}
        - Dữ liệu lỗi (Rescued)  : {rescued:,}
        """
        
        if extracted != inserted:
            logger.error(summary_msg)
            raise AirflowException(f"❌ Kéo {extracted:,} nhưng nạp {inserted:,}! Lệch pha dữ liệu.")
        else:
            logger.info(summary_msg)

    # Topology gọn gàng (Single Stream Flow)
    run_date_val = prepare_and_clean_target()
    total_extracted = extract_and_upload_full(run_date=run_date_val)
    inserted_data = full_load_stream_to_bronze(run_date=run_date_val, total_extracted=total_extracted)
    audited_data = audit_rescued_data_alert(inserted_data)
    reconciliation_summary(audited_data)

dag_instance = logistics_orders_full_load()