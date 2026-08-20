import os
from dataclasses import dataclass, field
from typing import Dict

@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "admin") # Trên production lấy từ Secret Manager
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "password")

@dataclass(frozen=True)
class NessieConfig:
    uri: str = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
    ref: str = os.getenv("NESSIE_REF", "main")
    auth_type: str = os.getenv("NESSIE_AUTH_TYPE", "NONE")
    warehouse_path: str = os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse/")

@dataclass(frozen=True)
class AppConfig:
    # Environment (dev/stg/prod)
    env: str = os.getenv("APP_ENV", "dev")
    
    # Khởi tạo các nhóm config con
    minio: MinioConfig = field(default_factory=MinioConfig)
    nessie: NessieConfig = field(default_factory=NessieConfig)
    
    # Table Mapping: Quản lý tên bảng Bronze/Silver tập trung
    tables: Dict[str, str] = field(default_factory=lambda: {
        "bronze_orders": "lakehouse.bronze.raw_orders",
        "silver_orders": "lakehouse.silver.orders",
        "silver_requests": "lakehouse.silver.order_requests",
        "silver_stops": "lakehouse.silver.order_stops"
    })
    
    # Tham số Lookback days mặc định (có thể bị ghi đè bởi Job Arguments)
    default_lookback_days: int = int(os.getenv("DEFAULT_LOOKBACK_DAYS", "3"))

# Khởi tạo Singleton Config để import dùng ở mọi nơi
config = AppConfig()