import os

# Bắt buộc sử dụng PostgreSQL từ biến môi trường. Nếu thiếu env sẽ báo lỗi ngay lập tức
if "SQLALCHEMY_DATABASE_URI" not in os.environ:
    raise KeyError("CRITICAL: Biến môi trường SQLALCHEMY_DATABASE_URI không tồn tại! Hệ thống bắt buộc sử dụng PostgreSQL.")

SQLALCHEMY_DATABASE_URI = os.environ["SQLALCHEMY_DATABASE_URI"]

# Khóa bảo mật hệ thống
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME")

# Tắt dữ liệu mẫu để giữ DB sạch sẽ
SUPERSET_LOAD_EXAMPLES = False

# Kích hoạt bảo mật API nội bộ
WTF_CSRF_ENABLED = True