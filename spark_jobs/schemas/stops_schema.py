from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    DoubleType, 
    IntegerType, 
    BooleanType, 
    ArrayType
)

def get_stops_schema() -> ArrayType:
    """
    Schema phức tạp cho payload JSON của raw_path_json.
    Chứa đầy đủ 26 thuộc tính của một Stop point.
    """
    stop_struct = StructType([
        # Thông tin cơ bản
        StructField("address", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("remarks", StringType(), True),
        StructField("building", StringType(), True),
        StructField("apt_number", StringType(), True),
        StructField("status", StringType(), True),
        
        # Nhóm tọa độ và thời gian hoàn thành (Group 3 timestamp gốc dạng Double unix)
        StructField("complete_time", DoubleType(), True),
        StructField("complete_lat", DoubleType(), True),
        StructField("complete_lng", DoubleType(), True),
        StructField("complete_comment", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("pod_info", StringType(), True),
        
        # Nhóm đánh giá (Group 2 Rating)
        StructField("rating_by_receiver", IntegerType(), True),
        StructField("comment_by_receiver", StringType(), True),
        
        # Nhóm thông tin thất bại
        StructField("fail_time", DoubleType(), True),
        StructField("fail_lat", DoubleType(), True),
        StructField("fail_lng", DoubleType(), True),
        StructField("fail_comment", StringType(), True),
        StructField("redelivery_note", StringType(), True),
        
        # Nhóm tài chính và yêu cầu xác thực
        StructField("cod", DoubleType(), True),
        StructField("tracking_number", StringType(), True),
        StructField("require_pod", BooleanType(), True),
        StructField("require_verification", BooleanType(), True)
    ])
    
    return ArrayType(stop_struct, containsNull=True)