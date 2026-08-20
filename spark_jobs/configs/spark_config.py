from pyspark.sql import SparkSession
from configs.app_config import config
from configs.logger_config import get_logger

logger = get_logger(__name__)

def get_spark_session(app_name: str) -> SparkSession:
    """
    Khởi tạo SparkSession chuẩn Production với Iceberg, Nessie và các tối ưu hiệu năng.
    """
    logger.info(f"Initializing SparkSession for App: {app_name} in {config.env.upper()} environment...")
    
    builder = SparkSession.builder.appName(app_name)
    
    # ---------------------------------------------------------
    # 1. Cấu hình Iceberg & Nessie Catalog (Kế thừa từ kiến trúc hiện tại)
    # ---------------------------------------------------------
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", config.nessie.uri) \
        .config("spark.sql.catalog.lakehouse.ref", config.nessie.ref) \
        .config("spark.sql.catalog.lakehouse.authentication.type", config.nessie.auth_type) \
        .config("spark.sql.catalog.lakehouse.warehouse", config.nessie.warehouse_path)

    # ---------------------------------------------------------
    # 2. Cấu hình S3/MinIO (Sử dụng credentials từ AppConfig)
    # ---------------------------------------------------------
    builder = builder \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", config.minio.endpoint) \
        .config("spark.hadoop.fs.s3a.endpoint", config.minio.endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", config.minio.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", config.minio.secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") # Tắt SSL nếu dùng MinIO nội bộ
    
    # ---------------------------------------------------------
    # 3. SENIOR TUNING: Tối ưu hiệu năng & Cấu hình mặc định
    # ---------------------------------------------------------

    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

    spark = builder.getOrCreate()

    # Log thông tin version và UI url để dễ debug
    logger.info(f"Spark initialized successfully. Version: {spark.version}")

    return spark