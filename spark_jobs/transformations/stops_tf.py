from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DecimalType, LongType, IntegerType, DoubleType
from schemas.stops_schema import get_stops_schema
from configs.logger_config import get_logger

logger = get_logger(__name__)

def transform_stops(df: DataFrame) -> DataFrame:
    """
    Thực thi logic transform cho bảng order_stops từ lớp Bronze -> Silver.
    Dựa trên logic của stg_order_stops.sql.
    """
    logger.info("Bắt đầu transform dữ liệu order_stops...")

    # ---------------------------------------------------------
    # BƯỚC 1: Lọc dữ liệu thô (Base Orders)
    # ---------------------------------------------------------
    # Loại bỏ các path null hoặc chuỗi rỗng/mảng rỗng
    filtered_df = df.filter(
        F.col("path").isNotNull() & 
        (~F.trim(F.col("path")).isin(['', '[]']))
    )

    # Đổi tên và ép kiểu các trường cơ bản từ Bronze trước khi parse JSON
    base_df = filtered_df.select(
        F.col("_id").cast(StringType()).alias("order_id"),
        F.col("_db_id").cast(LongType()).alias("db_id"),
        F.col("_source_system").cast(StringType()).alias("source_system"),
        F.from_unixtime(F.col("create_time").cast(DoubleType())).cast("timestamp").alias("create_time"),
        F.col("path").cast(StringType()).alias("raw_path_json"),
        F.col("_ingest_time")
    )

    # ---------------------------------------------------------
    # BƯỚC 2: Parse JSON thành Array(StructType) an toàn
    # ---------------------------------------------------------
    # Sử dụng from_json thay cho CAST(json_parse(...) AS ARRAY(ROW(...))) của Trino[cite: 2]
    stops_schema = get_stops_schema()
    parsed_df = base_df.withColumn(
        "stops_array", 
        F.from_json(F.col("raw_path_json"), stops_schema)
    )

    # ---------------------------------------------------------
    # BƯỚC 3: posexplode_outer để lấy dữ liệu + sinh index (tương đương UNNEST WITH ORDINALITY)
    # ---------------------------------------------------------
    # posexplode_outer tạo ra 2 cột: "pos" (chứa index, bắt đầu từ 0) và "stop_struct" (chứa JSON object)
    # Dùng _outer để giữ lại các dòng parse thất bại (giống LEFT JOIN UNNEST)[cite: 2]
    exploded_df = parsed_df.select(
        "*", 
        F.posexplode_outer(F.col("stops_array")).alias("stop_index_0_based", "stop_struct")
    )

    # Alias lại struct column để code phía dưới gọn gàng hơn
    t = F.col("stop_struct")

    # ---------------------------------------------------------
    # BƯỚC 4: Projection, Ép kiểu và Xử lý Null (COALESCE Groups)
    # ---------------------------------------------------------
    # Tạo danh sách các cột cần select để tránh chain withColumn gây phình execution plan
    final_columns = [
        F.col("source_system"),
        F.col("db_id"),
        F.col("order_id"),
        
        # Xử lý Stop Index: Trino WITH ORDINALITY bắt đầu từ 1, posexplode bắt đầu từ 0. 
        # Ta cộng thêm 1, và nếu Null (chưa parse được/rỗng) thì COALESCE về 0[cite: 2]
        F.coalesce(F.col("stop_index_0_based") + F.lit(1), F.lit(0)).cast(IntegerType()).alias("stop_index"),
        
        F.col("create_time"),
        
        # Nhóm thông tin Stop cơ bản[cite: 2]
        t.getItem("name").alias("stop_name"),
        t.getItem("mobile").alias("stop_mobile"),
        t.getItem("address").alias("stop_address"),
        t.getItem("building").alias("building"),
        t.getItem("apt_number").alias("apt_number"),
        
        # Nhóm Tọa độ[cite: 2]
        t.getItem("lat").alias("stop_lat"),
        t.getItem("lng").alias("stop_lng"),
        t.getItem("complete_lat").alias("complete_lat"),
        t.getItem("complete_lng").alias("complete_lng"),
        t.getItem("fail_lat").alias("fail_lat"),
        t.getItem("fail_lng").alias("fail_lng"),
        
        # Group 2: Tài chính -> COALESCE về 0.00[cite: 2]
        F.coalesce(t.getItem("cod").cast(DecimalType(18, 2)), F.lit(0.00)).alias("cod_amount"),
        F.upper(F.trim(t.getItem("status"))).alias("stop_status"),
        t.getItem("tracking_number").alias("tracking_number"),
        
        # Group 1: Categorical / Notes -> COALESCE về NO_NOTE[cite: 2]
        F.coalesce(t.getItem("remarks").cast(StringType()), F.lit('NO_NOTE')).alias("stop_remarks"),
        F.coalesce(t.getItem("redelivery_note").cast(StringType()), F.lit('NO_NOTE')).alias("redelivery_note"),
        
        # Flags yêu cầu xác thực[cite: 2]
        F.coalesce(t.getItem("require_pod"), F.lit(False)).alias("is_pod_required"),
        F.coalesce(t.getItem("require_verification"), F.lit(False)).alias("is_verification_required"),
        t.getItem("pod_info").alias("pod_info"),
        t.getItem("image_url").alias("pod_image_url"),
        
        # Group 2 (Rating & Comments): Khác biệt -> COALESCE[cite: 2]
        F.coalesce(t.getItem("rating_by_receiver"), F.lit(-1)).alias("rating_by_receiver"),
        F.coalesce(t.getItem("comment_by_receiver").cast(StringType()), F.lit('NO_NOTE')).alias("comment_by_receiver"),
        F.coalesce(t.getItem("complete_comment").cast(StringType()), F.lit('NO_NOTE')).alias("complete_comment"),
        F.coalesce(t.getItem("fail_comment").cast(StringType()), F.lit('NO_NOTE')).alias("fail_comment"),
        
        # Group 3: Timestamps -> from_unixtime và Giữ nguyên NULL[cite: 2]
        # Chú ý: Ở file configs/spark_config.py ta đã set TimeZone session mặc định là UTC, 
        # nên hàm from_unixtime sẽ tự động đồng bộ UTC mà không cần .cast("timestamp AT TIME ZONE 'UTC'")
        F.from_unixtime(t.getItem("complete_time")).cast("timestamp").alias("complete_time"),
        F.from_unixtime(t.getItem("fail_time")).cast("timestamp").alias("fail_time"),
        
        # Logic is_parse_failed: raw_path_json có dữ liệu nhưng stops_array lại Null[cite: 2]
        F.when(
            F.col("raw_path_json").isNotNull() & F.col("stops_array").isNull(), 
            True
        ).otherwise(False).alias("is_parse_failed"),
        
        F.col("_ingest_time"),
        F.current_timestamp().alias("_silver_updated_at")
    ]

    final_df = exploded_df.select(*final_columns)
    
    logger.info("Hoàn tất thiết lập logic transform order_stops.")
    return final_df