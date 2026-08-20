from pyspark.sql import SparkSession, DataFrame
from configs.logger_config import get_logger

logger = get_logger(__name__)

def upsert_iceberg_table(
    spark: SparkSession, 
    df: DataFrame, 
    target_table: str, 
    merge_keys: list,
    partition_cols: list = None
) -> None:
    """
    Hàm chuẩn Production thực thi UPSERT vào Iceberg table.
    - Nếu bảng chưa tồn tại: Tạo bảng mới và ghi đè toàn bộ (CTAS).
    - Nếu bảng đã tồn tại: Chạy lệnh MERGE INTO (Upsert).
    
    Args:
        spark: SparkSession object.
        df: DataFrame đầu ra đã được transform (Silver layer).
        target_table: Tên bảng Iceberg đích (VD: 'lakehouse.silver.orders').
        merge_keys: Danh sách unique keys dùng cho ON condition trong MERGE.
        partition_cols: (Tùy chọn) Danh sách cột dùng để partition khi tạo bảng lần đầu.
    """
    # 1. Khởi tạo / Full Load nếu bảng chưa tồn tại
    if not spark.catalog.tableExists(target_table):
        logger.info(f"Table {target_table} not found. Creating and writing initial data...")
        writer = df.write.format("iceberg").mode("overwrite")
        
        if partition_cols:
            # Iceberg hỗ trợ partition by days/months/hours thông qua SQL, 
            # nhưng dùng DataFrame API cần truyền thẳng tên cột (đã transform)
            writer = writer.partitionBy(*partition_cols)
            
        writer.saveAsTable(target_table)
        logger.info(f"Successfully created and loaded initial data into {target_table}")
        return

    # 2. Xử lý Incremental Merge (Upsert)
    temp_view = f"tmp_upsert_{target_table.split('.')[-1]}"
    df.createOrReplaceTempView(temp_view)
    
    # Tạo chuỗi điều kiện ON (Ví dụ: t.source_system = s.source_system AND t.db_id = s.db_id)
    merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
    
    # Query MERGE INTO ICEBERG
    merge_sql = f"""
        MERGE INTO {target_table} t
        USING {temp_view} s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    
    logger.info(f"Executing MERGE INTO {target_table} with keys {merge_keys}...")
    try:
        spark.sql(merge_sql)
        logger.info(f"Successfully merged data into {target_table}")
    except Exception as e:
        logger.error(f"Failed to merge data into {target_table}. Error: {str(e)}")
        raise e
    finally:
        # Dọn dẹp bộ nhớ: Drop temporary view sau khi merge xong
        spark.catalog.dropTempView(temp_view)