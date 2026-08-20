from pyspark.sql import DataFrame, Window, SparkSession
import pyspark.sql.functions as F
from datetime import timedelta
from py4j.protocol import Py4JJavaError
from configs.logger_config import get_logger

logger = get_logger(__name__)

def get_latest_records(df: DataFrame, partition_cols: list, order_col: str = "_ingest_time") -> DataFrame:
    """
    Loại bỏ trùng lặp (Deduplicate), giữ lại bản ghi mới nhất.
    """
    logger.info(f"Deduplicating data based on keys: {partition_cols}, ordered by {order_col} DESC")
    
    window_spec = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    
    dedup_df = df.withColumn("rn", F.row_number().over(window_spec)) \
                 .filter(F.col("rn") == 1) \
                 .drop("rn")
                 
    return dedup_df

def get_incremental_filter_time(spark: SparkSession, target_table: str, lookback_days: int = 1, time_col: str = "_ingest_time") -> str:
    """
    Lấy mốc thời gian để lọc dữ liệu Incremental từ Bronze.
    Bao gồm cơ chế Self-Healing (Tự động phục hồi) khi Nessie Catalog bị mất đồng bộ với Storage.
    """
    is_valid_table = False
    
    try:
        if spark.catalog.tableExists(target_table):
            # Ép Spark đọc file S3 để test
            spark.sql(f"SELECT 1 FROM {target_table} LIMIT 1").collect()
            is_valid_table = True
            
    except Py4JJavaError as e:
        error_msg = str(e.java_exception)
        if "FileNotFoundException" in error_msg or "NotFoundException" in error_msg:
            # SỬA Ở ĐÂY: Dừng cố gắng DROP TABLE bằng Spark SQL. 
            # Bắn Alert hướng dẫn DE can thiệp thủ công.
            logger.critical(f"🚨 STATE CORRUPTION DETECTED: Bảng '{target_table}' tồn tại trong Nessie Catalog nhưng mất dữ liệu trên MinIO/S3.")
            logger.critical(f"❌ Spark không thể tự phục hồi lỗi này. Bạn MỞ NESSIE UI hoặc dùng Nessie API để DELETE bảng '{target_table}', sau đó chạy lại Job để trigger Full Load.")
            raise RuntimeError(f"Corrupted Iceberg table state: {target_table}. Manual Nessie cleanup required.") from e
        else:
            raise e
            
    # Nếu bảng chưa từng tồn tại, hoặc vừa bị drop do state rác
    if not is_valid_table:
        logger.info(f"Bảng {target_table} không tồn tại hoặc vừa được reset. Job sẽ chạy ở chế độ Full Load.")
        return "1970-01-01 00:00:00"

    # Xử lý Incremental thông thường
    try:
        max_time_df = spark.sql(f"SELECT MAX({time_col}) as max_time FROM {target_table}")
        max_time_row = max_time_df.collect()[0]
        
        if max_time_row['max_time'] is None:
            return "1970-01-01 00:00:00"
            
        incremental_start_time = max_time_row['max_time'] - timedelta(days=lookback_days)
        formatted_time = incremental_start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Incremental mode: Lấy dữ liệu từ Bronze >= '{formatted_time}' (Đã lùi {lookback_days} ngày)")
        return formatted_time
        
    except Exception as e:
        logger.error(f"Lỗi truy vấn thời gian từ bảng {target_table}: {str(e)}")
        raise e