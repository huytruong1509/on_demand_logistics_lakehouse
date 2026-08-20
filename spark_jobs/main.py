import argparse
import sys
import time
import signal
import traceback
import os
from typing import Callable, Dict

from configs.app_config import config
from configs.logger_config import get_logger

# Import các job modules
from jobs import silver_stops_job
from jobs import silver_requests_job
from jobs import silver_orders_job

logger = get_logger(__name__)

# Type alias để IDE gợi ý code (Type Hinting)
JobFunction = Callable[[int], None]

# Mẫu thiết kế Registry (Factory Pattern) map tên string với function thực thi
JOB_REGISTRY: Dict[str, JobFunction] = {
    "silver_stops": silver_stops_job.run,
    "silver_requests": silver_requests_job.run,
    "silver_orders": silver_orders_job.run
}

def handle_sigterm(signum, frame):
    """
    Xử lý tín hiệu ngắt (SIGTERM/SIGINT) từ hệ điều hành hoặc Orchestrator (Airflow/K8s).
    Giúp pipeline không bị 'chết đứng', log lại nguyên nhân và exit an toàn.
    """
    logger.error(f"⚠️ Nhận được tín hiệu ngắt {signum} từ hệ thống. Đang tiến hành dừng job an toàn...")
    # 143 là mã exit chuẩn khi nhận SIGTERM trên môi trường Unix
    sys.exit(143)

def parse_args() -> argparse.Namespace:
    """
    Xử lý tham số đầu vào từ CLI. 
    Ưu tiên: CLI Arguments -> Environment Variables -> Default Config.
    """
    parser = argparse.ArgumentParser(
        description="PySpark Lakehouse Data Pipeline Entrypoint",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Sử dụng 'choices' để argparse tự động validate, thay vì if/else thủ công
    parser.add_argument(
        "--job-name", 
        type=str, 
        required=True,
        choices=list(JOB_REGISTRY.keys()),
        help="Tên của job cần chạy."
    )
    
    # Đọc fallback từ môi trường (hữu ích khi deploy qua Helm/K8s ConfigMap)
    default_lookback = int(os.environ.get("PIPELINE_LOOKBACK_DAYS", config.default_lookback_days))
    
    parser.add_argument(
        "--lookback-days", 
        type=int, 
        default=default_lookback, 
        help="Số ngày lùi lại để lấy dữ liệu incremental"
    )
    
    return parser.parse_args()

def main() -> None:
    # 1. Đăng ký signal handlers cho Graceful Shutdown
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # 2. Parse và chuẩn bị config
    args = parse_args()
    job_name = args.job_name.lower()
    lookback_days = args.lookback_days
    
    logger.info(f"🚀 Khởi động Pipeline. Cấu hình: Job='{job_name}', LookbackDays={lookback_days}")
    
    # 3. Định tuyến tới Job function (Đã được argparse bảo vệ bởi 'choices')
    job_function = JOB_REGISTRY[job_name]
    
    # 4. Đo lường hiệu suất (Observability)
    start_time = time.time()
    
    try:
        # Thực thi ETL Logic
        job_function(lookback_days=lookback_days)
        
        execution_time = time.time() - start_time
        logger.info(f"✅ Job '{job_name}' chạy THÀNH CÔNG. Tổng thời gian: {execution_time:.2f} giây.")
        sys.exit(0)
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"❌ Job '{job_name}' THẤT BẠI sau {execution_time:.2f} giây.")
        
        # Ghi toàn bộ Stack Trace ra hệ thống log để debug dễ dàng hơn
        logger.error(f"Chi tiết Call Stack:\n{traceback.format_exc()}")
        
        # Trả về mã lỗi 1 để Airflow/Kubernetes nhận diện là task FAILED
        sys.exit(1)

if __name__ == "__main__":
    main()