"""
Configuration for Logistics Orders ELT Pipeline
"""


# 1. Cấu hình API Nguồn (Dùng chung cho cả 2 luồng)
API_BASE_URL = "http://data_source:8005/api/v1"

# 2. Cấu hình MinIO (Landing Zone)
MINIO_CONN_ID = "minio_default"
LANDING_BUCKET = "lakehouse"
LANDING_PREFIX = "landing/logistics_orders"

# 3. Cấu hình Trino & Iceberg (Bronze Zone)
TRINO_CONN_ID = "trino_default"
TRINO_CATALOG = "lakehouse"
TRINO_SCHEMA = "bronze"
TARGET_TABLE = "raw_orders"

# 4. Cấu hình Nessie Catalog
NESSIE_CONN_ID = "nessie_api_default"