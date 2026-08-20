"""
Logistics Orders Incremental Load DAG
Nhiệm vụ: Kéo dữ liệu mới/cập nhật từ Orders API theo từng thành phố hàng ngày,
đảm bảo tính Idempotent (chạy lại không trùng dữ liệu) và nạp vào bảng Bronze trên Iceberg.
"""

import json
import logging
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag, task
from pendulum import datetime as pendulum_datetime
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.hooks.base import BaseHook

# Import cấu hình hạ tầng dùng chung
from config import (
    API_BASE_URL, MINIO_CONN_ID, LANDING_BUCKET, LANDING_PREFIX, 
    TRINO_CONN_ID, TRINO_CATALOG, TRINO_SCHEMA, TARGET_TABLE,
    NESSIE_CONN_ID
)

# Import các custom clients
from utils.orders_api_client import OrdersAPIClient
from utils.minio_loader import MinIOLoader
from utils.duckdb_api_to_bronze import DuckDBIcebergLoader

CITIES = ["SGN", "HAN"]
SCHEMA_FILE_PATH = Path(__file__).parent / "schemas" / "logistics_schema.json"

@dag(
    dag_id="logistics_orders_incremental_load",
    start_date=pendulum_datetime(2026, 1, 1, tz="UTC"),
    schedule_interval="@daily", 
    catchup=False, 
    max_active_runs=1,
    tags=["logistics", "incremental", "lakehouse", "batch"],
)
def logistics_orders_incremental_load():

    # ====================================================================================
    # TASK 1: CHUẨN BỊ MÔI TRƯỜNG & ĐẢM BẢO TÍNH IDEMPOTENT
    # ====================================================================================
    @task
    def prepare_environment(**context) -> Dict[str, str]:
        """
        Thiết lập thời gian và dọn dẹp môi trường 2 tầng (MinIO & Iceberg Bronze)
        để đảm bảo tính Idempotent (Chống duplicate khi Airflow Retry)[cite: 7].
        """
        logger = logging.getLogger(__name__)
        
        logical_date = context['logical_date']
        run_id = context['run_id'] 
        
        # Thiết lập cửa sổ kéo dữ liệu (vd: quét lại 3 ngày để bắt các update muộn)
        start_date = (logical_date - timedelta(days=3)).strftime('%Y-%m-%d')
        end_date = logical_date.strftime('%Y-%m-%d')
        run_date = logical_date.strftime('%Y-%m-%d')
        
        # 1. Dọn dẹp Tầng Landing (MinIO) để xóa sạch file JSON rác nếu task Extract bị fail trước đó[cite: 7]
        try:
            s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
            loader = MinIOLoader(s3_hook=s3_hook, bucket_name=LANDING_BUCKET, prefix=LANDING_PREFIX)
            loader.purge_landing_partition(run_date=run_date)
            logger.info(f"✅ [LANDING] Đã dọn dẹp sạch sẽ phân vùng run_date={run_date}.")
        except Exception as e:
            logger.error(f"❌ [LANDING] Lỗi nghiêm trọng khi dọn dẹp MinIO: {e}")
            raise AirflowException(f"Không thể dọn dẹp Landing Zone: {e}")

        # 2. Dọn dẹp Tầng Bronze (Iceberg qua Trino) để xóa dữ liệu lở dở của chính run_id này nếu DAG bị Retry[cite: 7]
        try:
            trino_hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
            
            # Lệnh DELETE áp dụng Partition Overwrite logic dựa trên run_id[cite: 7]
            delete_sql = f"""
                DELETE FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TARGET_TABLE}
                WHERE _run_id = '{run_id}'
            """
            logger.info(f"🧹 [BRONZE] Đang quét rác Iceberg cho _run_id = '{run_id}'...")
            
            trino_hook.run(delete_sql)
            logger.info("✅ [BRONZE] Môi trường Iceberg an toàn. Sẵn sàng Append data mới.")
            
        except Exception as e:
            # Bắt lỗi "Soft Fail" cho trường hợp bảng chưa tồn tại (Day 1)[cite: 7]
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                logger.warning(f"⚠️ [BRONZE] Bỏ qua dọn dẹp Iceberg vì bảng/schema chưa tồn tại: {e}")
            else:
                logger.warning(f"⚠️ [BRONZE] Lỗi dọn dẹp Iceberg (Vẫn tiếp tục pipeline): {e}")

        return {
            "run_date": run_date,
            "start_date": start_date,
            "end_date": end_date,
            "run_id": run_id
        }

    # ====================================================================================
    # TASK 2: DYNAMIC TASK MAPPING - EXTRACT VÀ UPLOAD (XỬ LÝ SONG SONG)
    # ====================================================================================
    @task(map_index_template="{{ task.op_kwargs['city_id'] }}")
    def extract_and_upload_city_incremental(city_id: str, env_data: Dict[str, str]) -> int:
        """
        [MAPPED TASK] Kéo dữ liệu và upload cho 1 thành phố duy nhất.
        Nếu thành phố này fail, Airflow chỉ retry thành phố này, không ảnh hưởng thành phố khác.
        """
        logger = logging.getLogger(__name__)
        
        run_date = env_data["run_date"]
        start_date = env_data["start_date"]
        end_date = env_data["end_date"]

        # Lọc danh sách cột nghiệp vụ và bỏ các cột hệ thống[cite: 7]
        with open(SCHEMA_FILE_PATH, "r") as f:
            schema_def = json.load(f)
            
        all_columns = schema_def.get("landing_layer", {}).get("columns", {}).keys()
        
        SYSTEM_EXCLUDE_COLS = {"_rescued_data", "_ingest_time", "_run_id", "_source_system"}
        expected_keys = {col for col in all_columns if col not in SYSTEM_EXCLUDE_COLS}

        api_client = OrdersAPIClient(base_url=API_BASE_URL)
        temp_dir = tempfile.gettempdir()
        file_path = Path(temp_dir) / f"inc_{run_date}_{city_id.lower()}_orders.json"
        
        total_extracted_rows = 0

        try:
            # Lấy data theo cơ chế Incremental
            orders = api_client.get_incremental_orders(
                city_id=city_id, 
                start_date=start_date, 
                end_date=end_date
            )
            
            with open(file_path, "w", encoding="utf-8") as f:
                for record in orders:
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

                    # Đóng gói các trường lạ rác vào _rescued_data
                    if unknown_fields:
                        processed_record["_rescued_data"] = json.dumps(unknown_fields, ensure_ascii=False)

                    f.write(json.dumps(processed_record, ensure_ascii=False) + "\n")
                    total_extracted_rows += 1 

            if total_extracted_rows > 0:
                s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
                loader = MinIOLoader(s3_hook=s3_hook, bucket_name=LANDING_BUCKET, prefix=LANDING_PREFIX)
                logger.info(f"Uploading {file_path.name} ({total_extracted_rows} rows) lên Landing...")
                loader.upload_incremental_files({city_id: str(file_path)}, run_date)
            else:
                logger.info(f"Không có dữ liệu mới cho {city_id} trong khoảng {start_date} -> {end_date}[cite: 7].")

        except Exception as e:
            logger.error(f"Lỗi khi xử lý thành phố {city_id}: {e}")
            raise AirflowException(f"Task Failed cho thành phố {city_id}")
        finally:
            if os.path.exists(file_path): 
                os.remove(file_path)
                logger.info(f"Đã dọn dẹp an toàn file tạm: {file_path}[cite: 7]")

        return total_extracted_rows

    # ====================================================================================
    # TASK 3: REDUCE STAGE - TỔNG HỢP KẾT QUẢ TỪ CÁC TASK SONG SONG
    # ====================================================================================
    @task
    def aggregate_metrics(extracted_counts: List[int]) -> int:
        """Tổng hợp toàn bộ số dòng kéo được từ tất cả các worker (Map-Reduce pattern)[cite: 7]."""
        total = sum(extracted_counts)
        logging.getLogger(__name__).info(f"🚀 TỔNG HỢP: Toàn bộ các worker đã hoàn thành. Tổng số rows: {total}")
        return total

    # ====================================================================================
    # TASK 4: LOAD TỪ LANDING VÀO BRONZE ICEBERG BẰNG DUCKDB
    # ====================================================================================
    @task
    def incremental_insert_to_bronze(env_data: Dict[str, str], total_extracted: int, **context) -> Dict[str, Any]:
        logger = logging.getLogger(__name__)
        run_id = context['run_id']
        run_date = env_data["run_date"]
        
        # Nếu không có dòng dữ liệu nào, bỏ qua để tiết kiệm I/O[cite: 7]
        if total_extracted == 0:
            logger.info("Không có dữ liệu mới từ API. Bỏ qua bước nạp Bronze[cite: 7].")
            return {
                "inserted_run_id": run_id,
                "total_inserted_rows": 0,
                "total_extracted_rows": 0
            }

        try:
            minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
            nessie_conn = BaseHook.get_connection(NESSIE_CONN_ID)
            nessie_uri = f"http://{nessie_conn.host}:{nessie_conn.port}/iceberg"
        except Exception as e:
            raise AirflowException(f"Lỗi lấy cấu hình Connection: {e}")

        try:
            with open(SCHEMA_FILE_PATH, "r") as f:
                schema_def = json.load(f)
            bronze_columns = schema_def["bronze_layer"]["columns"]
        except Exception as e:
            raise AirflowException("Hỏng định nghĩa Schema JSON.")

        with DuckDBIcebergLoader(
            s3_endpoint=minio_conn.extra_dejson.get("endpoint_url", "http://minio:9000"),
            s3_access_key=minio_conn.login,
            s3_secret_key=minio_conn.password,
            nessie_uri=nessie_uri
        ) as loader:
            inserted_rows = loader.load_incremental(
                bucket=LANDING_BUCKET,
                prefix=LANDING_PREFIX,
                target_schema=TRINO_SCHEMA,
                target_table=TARGET_TABLE,
                run_id=run_id,
                run_date=run_date,
                bronze_columns=bronze_columns,
                source_system="ORDERS_API"
            )
        
        return {
            "inserted_run_id": run_id,
            "total_inserted_rows": inserted_rows,
            "total_extracted_rows": total_extracted
        }

    # ====================================================================================
    # TASK 5 & 6: AUDIT & ĐỐI SOÁT
    # ====================================================================================
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
        records = trino_hook.get_first(audit_sql)
        rescued_count = records[0] if records else 0
        
        if rescued_count > 0:
            logger.error(f"🚨 [CẢNH BÁO SCHEMA DRIFT] Phát hiện {rescued_count} dòng chứa dữ liệu rác trong Run ID: {inserted_run_id}[cite: 7].")
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
        📊 BÁO CÁO ĐỐI SOÁT DỮ LIỆU
        - Run ID: {run_id}
        - Số dòng kéo từ API (Extracted)     : {extracted:,}
        - Số dòng nạp vào Bronze (Inserted)  : {inserted:,}
        - Số dòng dính Schema Drift (Rescued): {rescued:,}
        """
        # Kiểm tra đối soát và báo lỗi ngầm nếu có sự khác biệt giữa lượng trích xuất và nạp vào[cite: 7]
        if extracted != inserted:
            logger.error(summary_msg)
            raise AirflowException(f"❌ LỖI NGẦM (SILENT FAILURE): Kéo về {extracted} nhưng nạp {inserted}![cite: 7]")
        else:
            logger.info(summary_msg)

    # ---------------------------------------------------------
    # WIRING TASKS (Kết nối các luồng dữ liệu - DAG Topology)
    # ---------------------------------------------------------
    env_data = prepare_environment()
    
    # Dùng .partial() để chốt tham số chung, .expand() để duyệt list thành phố tạo Task song song[cite: 7]
    mapped_extraction = extract_and_upload_city_incremental.partial(env_data=env_data).expand(city_id=CITIES)
    
    total_extracted_rows = aggregate_metrics(mapped_extraction)
    
    inserted_data = incremental_insert_to_bronze(env_data=env_data, total_extracted=total_extracted_rows)
    
    audited_data = audit_rescued_data_alert(inserted_data)
    
    reconciliation_summary(audited_data)

dag_instance = logistics_orders_incremental_load()