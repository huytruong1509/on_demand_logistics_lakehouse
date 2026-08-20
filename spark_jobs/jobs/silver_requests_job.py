from configs.spark_config import get_spark_session
from configs.app_config import config
from configs.logger_config import get_logger
from utils.spark_utils import get_latest_records, get_incremental_filter_time
from utils.iceberg_utils import upsert_iceberg_table
from transformations.requests_tf import transform_requests
import pyspark.sql.functions as F

logger = get_logger(__name__)

def run(lookback_days: int) -> None:
    """
    Thực thi luồng ETL chuyển đổi dữ liệu order_requests từ Bronze sang Silver.
    Dựa trên cấu hình Incremental & Merge của dbt stg_order_requests.
    """
    job_name = "Job_Silver_Order_Requests"
    spark = get_spark_session(job_name)
    
    source_table = config.tables["bronze_orders"]
    target_table = config.tables["silver_requests"]
    
    try:
        logger.info(f"--- BẮT ĐẦU CHẠY JOB: {job_name} ---")
        
        # 1. Tính toán mốc thời gian Incremental[cite: 1]
        incremental_time = get_incremental_filter_time(spark, target_table, lookback_days)
        
        # 2. Đọc dữ liệu thô (Predicate Pushdown để tối ưu I/O)
        logger.info(f"Đọc dữ liệu từ {source_table} với _ingest_time >= '{incremental_time}'")
        raw_df = spark.table(source_table).filter(F.col("_ingest_time") >= F.lit(incremental_time))
        
        # Chặn sớm nếu không có dữ liệu mới để tiết kiệm tài nguyên tính toán
        if raw_df.isEmpty():
            logger.info("Không có dữ liệu mới trong Bronze layer. Kết thúc Job thành công.")
            return

        # 3. Loại bỏ trùng lặp ở Bronze (Deduplication)[cite: 1]
        # Logic: ROW_NUMBER() OVER(PARTITION BY _source_system, _db_id, _id ORDER BY _ingest_time DESC) = 1
        dedup_df = get_latest_records(
            df=raw_df,
            partition_cols=["_source_system", "_db_id", "_id"],
            order_col="_ingest_time"
        )
        
        # 4. Thực thi Business Logic (Transformations)
        # Quá trình lọc JSON, parse StructType và explode diễn ra tại đây
        silver_df = transform_requests(dedup_df)
        
        # 5. Ghi vào Iceberg Silver (Merge / Upsert)
        # Các cột Unique Key được định nghĩa trong file SQL gốc[cite: 1]
        merge_keys = ["source_system", "db_id", "order_id", "request_id"]
        
        upsert_iceberg_table(
            spark=spark,
            df=silver_df,
            target_table=target_table,
            merge_keys=merge_keys
            # Ghi chú: Nếu hệ thống chưa có bảng, có thể truyền thêm partition_cols=["create_time_day"] 
            # (nếu đã tạo thêm cột phái sinh day(create_time) ở bước transform để mô phỏng dbt config[cite: 1])
        )
        
        logger.info(f"--- KẾT THÚC THÀNH CÔNG JOB: {job_name} ---")
        
    except Exception as e:
        logger.error(f"Job {job_name} thất bại với lỗi: {str(e)}", exc_info=True)
        # Ném exception ra ngoài để Airflow/Orchestrator capture được trạng thái FAILED
        raise e 
    finally:
        # Bắt buộc đóng SparkSession để trả lại memory/vCores cho Cluster
        logger.info("Đóng SparkSession. Giải phóng tài nguyên.")
        spark.stop()