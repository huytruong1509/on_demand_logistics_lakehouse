from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType, DoubleType, LongType, IntegerType, 
    DecimalType, TimestampType, BooleanType
)
from configs.logger_config import get_logger

logger = get_logger(__name__)

def transform_orders(df: DataFrame) -> DataFrame:
    """
    Thực thi logic transform cho bảng orders từ lớp Bronze -> Silver.
    Dựa trên cấu hình dbt: stg_orders.sql
    """
    logger.info("Bắt đầu transform dữ liệu orders...")

    # ---------------------------------------------------------
    # Helper Functions (DRY Principle)
    # ---------------------------------------------------------
    def parse_unix_timestamp(col_name: str):
        # TimeZone đã được config là UTC ở mức Session, nên chỉ cần cast sang timestamp
        return F.from_unixtime(F.col(col_name).cast(DoubleType())).cast(TimestampType()).alias(col_name)

    def parse_financial_metric(col_name: str, alias_name: str = None):
        if alias_name is None:
            alias_name = col_name
        return F.coalesce(F.col(col_name).cast(DecimalType(18, 2)), F.lit(0.00)).alias(alias_name)

    # ---------------------------------------------------------
    # Column Projections
    # ---------------------------------------------------------
    final_columns = [
        # Key Identifiers
        F.col("_id").cast(StringType()).alias("order_id"),
        F.col("_db_id").cast(LongType()).alias("db_id"),
        F.col("_source_system").cast(StringType()).alias("source_system"),
        
        # Order Status & Service Identifiers (Group 1)
        F.coalesce(F.upper(F.trim(F.col("status").cast(StringType()))), F.lit('UNKNOWN')).alias("status"),
        F.coalesce(F.upper(F.trim(F.col("sub_status").cast(StringType()))), F.lit('UNKNOWN')).alias("sub_status"),
        F.col("service_id").cast(StringType()).alias("service_id"),
        F.coalesce(F.upper(F.trim(F.col("city_id").cast(StringType()))), F.lit('UNKNOWN')).alias("city_id"),
        
        # Entities & Actors (Group 1)[cite: 3]
        F.col("user_id").cast(StringType()).alias("user_id"),
        F.col("user_name").cast(StringType()).alias("user_name"),
        F.col("supplier_id").cast(StringType()).alias("supplier_id"),
        F.col("supplier_name").cast(StringType()).alias("supplier_name"),
        F.coalesce(F.col("partner").cast(StringType()), F.lit('NON_PARTNER')).alias("partner"),
        F.col("assigned_by").cast(StringType()).alias("assigned_by"),
        
        # Timestamps (Group 3: TUYỆT ĐỐI KHÔNG COALESCE)[cite: 3]
        parse_unix_timestamp("create_time"),
        parse_unix_timestamp("order_time"),
        parse_unix_timestamp("idle_until"),
        parse_unix_timestamp("accept_time"),
        parse_unix_timestamp("board_time"),
        parse_unix_timestamp("pickup_time"),
        parse_unix_timestamp("complete_time"),
        parse_unix_timestamp("cancel_time"),
        parse_unix_timestamp("return_time"),
        
        # Spatial & Acceptance Metrics (Group 2)[cite: 3]
        F.col("accept_lat").cast(DoubleType()).alias("accept_lat"),
        F.col("accept_lng").cast(DoubleType()).alias("accept_lng"),
        F.coalesce(F.col("accept_distance").cast(DoubleType()), F.lit(0.0)).alias("accept_distance"),
        F.coalesce(F.col("accept_duration").cast(DoubleType()), F.lit(0.0)).alias("accept_duration"),
        
        # Cancellation Notes (Group 1)[cite: 3]
        F.coalesce(F.col("cancel_comment").cast(StringType()), F.lit('NOT_CANCELLED')).alias("cancel_comment"),
        F.coalesce(F.col("cancel_by_user").cast(StringType()), F.lit('NOT_CANCELLED')).alias("cancel_by_user"),
        
        # Financials & Pricing Breakdown (Group 2)[cite: 3]
        F.coalesce(F.upper(F.trim(F.col("currency").cast(StringType()))), F.lit('VND')).alias("currency"),
        F.coalesce(F.col("promo_code").cast(StringType()), F.lit('NO_PROMO')).alias("promo_code"),
        F.coalesce(F.upper(F.trim(F.col("payment_method").cast(StringType()))), F.lit('UNKNOWN')).alias("payment_method"),
        
        F.coalesce(F.col("distance").cast(DoubleType()), F.lit(0.0)).alias("distance_km"),
        
        parse_financial_metric("distance_fee"),
        parse_financial_metric("stop_fee"),
        parse_financial_metric("request_fee"),
        parse_financial_metric("discount", "discount_amount"),
        parse_financial_metric("total_fee"),
        parse_financial_metric("distance_price"),
        parse_financial_metric("special_request_price"),
        parse_financial_metric("stoppoint_price"),
        parse_financial_metric("voucher_discount"),
        parse_financial_metric("subtotal_price"),
        parse_financial_metric("total_price"),
        parse_financial_metric("total_pay"),
        parse_financial_metric("user_main_account"),
        parse_financial_metric("user_bonus_account"),
        parse_financial_metric("supplier_main_account"),
        parse_financial_metric("supplier_bonus_account"),
        
        # Ratings & Comments[cite: 3]
        F.coalesce(F.col("rating_by_user").cast(IntegerType()), F.lit(-1)).alias("rating_by_user"),
        F.coalesce(F.col("comment_by_user").cast(StringType()), F.lit('NO_NOTE')).alias("comment_by_user"),
        F.coalesce(F.col("rating_by_supplier").cast(IntegerType()), F.lit(-1)).alias("rating_by_supplier"),
        F.coalesce(F.col("comment_by_supplier").cast(StringType()), F.lit('NO_NOTE')).alias("comment_by_supplier"),
        
        # Additional Info[cite: 3]
        F.coalesce(F.col("remarks").cast(StringType()), F.lit('NO_NOTE')).alias("order_remarks"),
        F.coalesce(F.col("remind").cast(BooleanType()), F.lit(False)).alias("is_reminded"),
        F.coalesce(F.col("index").cast(IntegerType()), F.lit(0)).alias("order_index"),
        
        # GeoJSON Extraction bằng get_json_object thay vì json_extract_scalar của Trino[cite: 3]
        F.get_json_object(F.col("from_location"), "$.coordinates[1]").cast(DoubleType()).alias("from_lat"),
        F.get_json_object(F.col("from_location"), "$.coordinates[0]").cast(DoubleType()).alias("from_lng"),
        
        # Giữ nguyên kiểu String cho các cấu trúc phức tạp Iceberg không hỗ trợ[cite: 3]
        # F.col("requests").cast(StringType()).alias("requests_json_array"),
        F.col("_rescued_data").cast(StringType()).alias("_rescued_data"),
        
        # Metadata[cite: 3]
        F.col("_ingest_time"),
        F.col("_run_id"),
        F.current_timestamp().alias("_silver_updated_at")
    ]

    # Thực thi projection duy nhất
    final_df = df.select(*final_columns)
    
    logger.info("Hoàn tất thiết lập logic transform orders.")
    return final_df