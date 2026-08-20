import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Khởi tạo và cấu hình Logger chuẩn Production.
    """
    logger = logging.getLogger(name)
    
    # Chỉ add handler nếu logger chưa có (tránh duplicate log trong Spark)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format log: Thời gian | Level | Module | Message
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Log ra Stdout để Docker/Airflow dễ dàng capture
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        
        # Ngăn chặn log lan truyền (propagate) lên root logger của Spark gây nhiễu
        logger.propagate = False

    return logger