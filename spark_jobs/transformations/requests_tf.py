from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType, IntegerType, DecimalType
from schemas.requests_schema import get_requests_schema
from configs.logger_config import get_logger

logger = get_logger(__name__)

def transform_requests(df: DataFrame) -> DataFrame:
    """
    Thực thi logic transform cho bảng order_requests từ lớp Bronze -> Silver.
    Dựa trên logic của dbt: stg_order_requests.sql.
    """
    logger.info("Bắt đầu transform dữ liệu order_requests...")

    # ---------------------------------------------------------
    # BƯỚC 1: Lọc dữ liệu thô (Base Orders)
    # ---------------------------------------------------------
    # Lọc nhanh các chuỗi rỗng và mảng rỗng
    filtered_df = df.filter(
        F.col("requests").isNotNull() & 
        (~F.trim(F.col("requests")).isin(['', '[]']))
    )

    # Ép kiểu dữ liệu cơ bản từ Bronze
    base_df = filtered_df.select(
        F.col("_id").cast(StringType()).alias("order_id"),
        F.col("_db_id").cast(LongType()).alias("db_id"),
        F.col("_source_system").cast(StringType()).alias("source_system"),
        F.from_unixtime(F.col("create_time").cast(DoubleType())).cast("timestamp").alias("create_time"),
        F.col("requests").cast(StringType()).alias("raw_requests_json"),
        F.col("_ingest_time")
    )

    # ---------------------------------------------------------
    # BƯỚC 2: Parse JSON an toàn bằng StructType
    # ---------------------------------------------------------
    requests_schema = get_requests_schema()
    parsed_df = base_df.withColumn(
        "requests_array",
        F.from_json(F.col("raw_requests_json"), requests_schema)
    )

    # ---------------------------------------------------------
    # BƯỚC 3: Explode (Tương đương CROSS JOIN UNNEST)
    # ---------------------------------------------------------
    # Sử dụng explode (thay vì explode_outer) vì SQL gốc dùng CROSS JOIN UNNEST,
    # nghĩa là nếu mảng rỗng hoặc null, dòng order đó sẽ bị loại bỏ khỏi bảng requests.
    exploded_df = parsed_df.select(
        "*",
        F.explode(F.col("requests_array")).alias("req")
    )

    # Lọc bỏ các phần tử trong mảng bị rỗng id (tương đương: AND t._id IS NOT NULL)[cite: 1]
    valid_requests_df = exploded_df.filter(F.col("req._id").isNotNull())

    # ---------------------------------------------------------
    # BƯỚC 4: Projection, Xử lý tài chính (Coalesce & Cast)
    # ---------------------------------------------------------
    final_columns = [
        F.col("source_system"),
        F.col("db_id"),
        F.col("order_id"),
        
        # Bóc tách thuộc tính từ struct "req"
        F.col("req._id").alias("request_id"),
        
        # Xử lý các trường số học, nếu null trả về 0 / 0.00 như SQL gốc[cite: 1]
        F.coalesce(F.col("req.price").cast(DecimalType(18, 2)), F.lit(0.00)).alias("request_price"),
        F.coalesce(F.col("req.value").cast(DecimalType(18, 2)), F.lit(0.00)).alias("request_value"),
        F.coalesce(F.col("req.num").cast(IntegerType()), F.lit(0)).alias("request_num"),
        
        F.col("create_time"),
        F.col("_ingest_time"),
        F.current_timestamp().alias("_silver_updated_at")
    ]

    final_df = valid_requests_df.select(*final_columns)
    
    logger.info("Hoàn tất thiết lập logic transform order_requests.")
    return final_df