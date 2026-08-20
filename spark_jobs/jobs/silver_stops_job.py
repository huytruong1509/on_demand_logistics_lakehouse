from configs.spark_config import get_spark_session
from configs.app_config import config
from configs.logger_config import get_logger
from utils.spark_utils import get_latest_records, get_incremental_filter_time
from utils.iceberg_utils import upsert_iceberg_table
from transformations.stops_tf import transform_stops
import pyspark.sql.functions as F

logger = get_logger(__name__)

def run(lookback_days: int) -> None:
    """
    Thực thi luồng ETL chuyển đổi dữ liệu order_stops từ Bronze sang Silver.
    """
    job_name = "Job_Silver_Order_Stops"
    spark = get_spark_session(job_name)
    
    source_table = config.tables["bronze_orders"]
    target_table = config.tables["silver_stops"]
    
    try:
        logger.info(f"--- BẮT ĐẦU CHẠY JOB: {job_name} ---")
        
        # 1. Tính toán mốc thời gian Incremental[cite: 2]
        incremental_time = get_incremental_filter_time(spark, target_table, lookback_days)
        
        # 2. Đọc dữ liệu thô (có filter đẩy xuống storage - Predicate Pushdown)
        logger.info(f"Reading data from {source_table} with _ingest_time >= '{incremental_time}'")
        raw_df = spark.table(source_table).filter(F.col("_ingest_time") >= F.lit(incremental_time))
        
        if raw_df.isEmpty():
            logger.info("Không có dữ liệu mới. Bỏ qua các bước transform và kết thúc Job thành công.")
            return

        # 3. Loại bỏ trùng lặp ở Bronze (Deduplication)[cite: 2]
        # Tương đương ROW_NUMBER() OVER(PARTITION BY _source_system, _db_id, _id ORDER BY _ingest_time DESC) = 1
        dedup_df = get_latest_records(
            df=raw_df,
            partition_cols=["_source_system", "_db_id", "_id"],
            order_col="_ingest_time"
        )
        
        # 4. Thực thi Business Logic (Transformations)
        silver_df = transform_stops(dedup_df)
        
        # 5. Ghi vào Iceberg Silver (Merge / Upsert)[cite: 2]
        # Khai báo Unique Keys cho bảng stops
        merge_keys = ["source_system", "db_id", "order_id", "stop_index"]
        
        upsert_iceberg_table(
            spark=spark,
            df=silver_df,
            target_table=target_table,
            merge_keys=merge_keys
            # Lưu ý: Nếu muốn tận dụng Iceberg Hidden Partitioning như dbt (day(create_time)), 
            # có thể cấu hình DDL tạo bảng sẵn trước bằng Airflow, Upsert sẽ tự động nhận diện.
        )
        
        logger.info(f"--- KẾT THÚC THÀNH CÔNG JOB: {job_name} ---")
        
    except Exception as e:
        logger.error(f"Job {job_name} thất bại với lỗi: {str(e)}", exc_info=True)
        # Re-raise exception để Airflow (tầng ngoài) nhận được signal FAILED thay vì SUCCESS giả
        raise e 
    finally:
        # Bắt buộc phải stop session để giải phóng tài nguyên YARN/Kubernetes
        logger.info("Dừng SparkSession và dọn dẹp tài nguyên.")
        spark.stop()