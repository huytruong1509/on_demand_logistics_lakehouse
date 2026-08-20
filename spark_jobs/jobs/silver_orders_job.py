from configs.spark_config import get_spark_session
from configs.app_config import config
from configs.logger_config import get_logger
from utils.spark_utils import get_latest_records, get_incremental_filter_time
from utils.iceberg_utils import upsert_iceberg_table
from transformations.orders_tf import transform_orders
import pyspark.sql.functions as F

logger = get_logger(__name__)

def run(lookback_days: int) -> None:
    """
    Thực thi luồng ETL chuyển đổi dữ liệu orders (Bảng chính) từ Bronze sang Silver.
    """
    job_name = "Job_Silver_Orders"
    spark = get_spark_session(job_name)
    
    source_table = config.tables["bronze_orders"]
    target_table = config.tables["silver_orders"]
    
    try:
        logger.info(f"--- BẮT ĐẦU CHẠY JOB: {job_name} ---")
        
        # 1. Tính toán mốc thời gian Incremental
        incremental_time = get_incremental_filter_time(spark, target_table, lookback_days)
        
        # 2. Đọc dữ liệu thô (Predicate Pushdown để tối ưu I/O storage)
        logger.info(f"Đọc dữ liệu từ {source_table} với _ingest_time >= '{incremental_time}'")
        raw_df = spark.table(source_table).filter(F.col("_ingest_time") >= F.lit(incremental_time))
        
        # Chặn sớm nếu không có dữ liệu mới (Tiết kiệm vCores)
        if raw_df.isEmpty():
            logger.info("Không có dữ liệu đơn hàng mới. Bỏ qua transform và kết thúc Job thành công.")
            return

        # 3. Loại bỏ trùng lặp ở Bronze (Deduplication)
        # Giữ lại bản ghi mới nhất dựa trên _ingest_time
        dedup_df = get_latest_records(
            df=raw_df,
            partition_cols=["_source_system", "_db_id", "_id"],
            order_col="_ingest_time"
        )
        
        # 4. Thực thi Business Logic (Transformations)
        # Ép kiểu 60+ cột, chuẩn hóa timestamps, format JSON coordinates...
        silver_df = transform_orders(dedup_df)
        
        # 5. Ghi vào Iceberg Silver (Merge / Upsert)
        # Định nghĩa Unique Keys cho bảng orders (Bảng cha nên không có index phụ)
        merge_keys = ["source_system", "db_id", "order_id"]
        
        upsert_iceberg_table(
            spark=spark,
            df=silver_df,
            target_table=target_table,
            merge_keys=merge_keys
        )
        
        logger.info(f"--- KẾT THÚC THÀNH CÔNG JOB: {job_name} ---")
        
    except Exception as e:
        logger.error(f"Job {job_name} thất bại với lỗi: {str(e)}", exc_info=True)
        raise e 
    finally:
        # Đảm bảo giải phóng tài nguyên Cluster
        logger.info("Đóng SparkSession. Dọn dẹp tài nguyên.")
        spark.stop()