import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------
# CẤU HÌNH MÔI TRƯỜNG DEV (Lấy từ .env của Airflow)
# ---------------------------------------------------------
# Giữ lại đường dẫn thư mục cá nhân như file gốc để code thay đổi trên máy tính 
# sẽ phản ánh ngay vào Docker container mà không cần build lại Image.
HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH", "D:/logistics-lakehouse/spark_jobs")

# Thông tin kết nối MinIO và Nessie (Mặc định dùng local service)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# ---------------------------------------------------------
# CẤU HÌNH AIRFLOW DAG
# ---------------------------------------------------------
default_args = {
    'owner': 'data_engineering_dev',
    'depends_on_past': False,
    'retries': 1, # Trong lúc dev, fail 1 lần rồi chạy lại là đủ để test
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="serverless_spark_dev_pipeline",
    default_args=default_args,
    description="Local Dev DAG - Bronze to Silver Lakehouse",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval=None, # Chạy thủ công (on-demand) phù hợp cho lúc dev
    catchup=False,
    max_active_runs=1,
    tags=["dev", "lakehouse", "silver"]
) as dag:

    # Khai báo các job dựa trên JOB_REGISTRY trong main.py của chúng ta
    # Khai báo các job dựa trên JOB_REGISTRY
    silver_jobs = [
        "silver_orders",
        "silver_requests",
        "silver_stops"
    ]

    # Tạo một biến tạm để lưu task trước đó, dùng cho việc nối dependencies
    previous_task = None

    for job_name in silver_jobs:
        
        spark_command_list = [
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            
            # Tăng tài nguyên cấp phát cho JVM
            "--driver-memory", "4g",
            "--conf", "spark.driver.memoryOverhead=1g",
            
            # Cấp bộ nhớ Off-heap cho Arrow / Zstd để tránh crash
            "--conf", "spark.memory.offHeap.enabled=true",
            "--conf", "spark.memory.offHeap.size=1g",
            
            # Tắt Vectorized Reader để fix triệt để lỗi SIGSEGV JVM crash
            "--conf", "spark.sql.parquet.enableVectorizedReader=false",
            "--conf", "spark.sql.iceberg.vectorization.enabled=false",

            # Giữ nguyên các config Java Options
            "--conf", "spark.driver.extraJavaOptions=-XX:-ShowCodeDetailsInExceptionMessages -Dio.netty.tryReflectionSetAccessible=true -XX:-OmitStackTraceInFastThrow",
            "--conf", "spark.executor.extraJavaOptions=-XX:-ShowCodeDetailsInExceptionMessages -Dio.netty.tryReflectionSetAccessible=true -XX:-OmitStackTraceInFastThrow",
            
            # Submit file python
            "/app/main.py",
            "--job-name", job_name,
            "--lookback-days", "3"
        ]

        # 2. CẤU HÌNH DOCKER OPERATOR
        current_task = DockerOperator(
            task_id=f"run_{job_name}_dev",
            image='logistics-lakehouse-spark:latest',
            container_name=f'ephemeral_spark_{job_name}_{{{{ ts_nodash }}}}',
            api_version='auto',
            
            # ĐỔI THÀNH 'success': Chỉ tự động xóa container nếu job chạy TỐT.
            # Nếu FAIL, container sẽ được giữ lại để bạn vào check file hs_err_pid1.log
            auto_remove='success', 

            mem_limit='8g', 
            
            network_mode='lakehouse_net',
            mount_tmp_dir=False,
            command=spark_command_list,
            mounts=[
                # Mount code của bạn
                Mount(
                    source=HOST_PROJECT_PATH,
                    target="/app",
                    type="bind"
                ),
                # MOUNT THÊM THƯ MỤC ĐỂ HỨNG LOG CRASH (Tạo folder 'logs' trong HOST_PROJECT_PATH)
                Mount(
                    source=f"{HOST_PROJECT_PATH}/logs", 
                    target="/opt/spark/work-dir", 
                    type="bind"
                )
            ],
            docker_url='unix://var/run/docker.sock',
            environment={
                'MINIO_ENDPOINT': MINIO_ENDPOINT,
                'NESSIE_URI': NESSIE_URI,
                'MINIO_ACCESS_KEY': MINIO_ACCESS_KEY,
                'MINIO_SECRET_KEY': MINIO_SECRET_KEY,
                'PYTHONPATH': '/app'
            }
        )

        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task