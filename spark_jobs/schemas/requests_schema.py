from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    DoubleType, 
    IntegerType, 
    ArrayType
)

def get_requests_schema() -> ArrayType:
    """
    Schema cho payload JSON của raw_requests_json.
    Dữ liệu đầu vào là một mảng các object (Array of Structs).
    """
    request_struct = StructType([
        StructField("_id", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("value", DoubleType(), nullable=True),
        StructField("num", IntegerType(), nullable=True)
    ])
    
    # Do dữ liệu là mảng JSON, ta bọc trong ArrayType
    return ArrayType(request_struct, containsNull=True)