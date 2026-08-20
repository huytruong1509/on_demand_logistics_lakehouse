import os
import logging
import pandas as pd
import sqlglot
from sqlglot import exp
from trino.dbapi import connect
from trino.exceptions import TrinoExternalError, TrinoQueryError, TrinoUserError

# Cấu hình Logger
logger = logging.getLogger(__name__)

class SecurityGuardrailException(Exception):
    """Ngoại lệ tùy chỉnh ném ra khi SQL vi phạm chính sách bảo mật."""
    pass

class TrinoExecutor:
    """
    Engine thực thi SQL trên Trino chuẩn Production.
    Tích hợp Static Query Validation và Auto-Rewriting qua AST.
    """
    def __init__(self):
        # Nạp cấu hình từ biến môi trường
        self.host = os.getenv("TRINO_HOST", "trino-coordinator")
        self.port = int(os.getenv("TRINO_PORT", "8080"))
        
        # [FIX BUG] Sửa lại mặc định khớp với file rules.json (ai_read_only)
        self.user = os.getenv("TRINO_USER", "ai_read_only")
        
        # [FIX BUG] Trỏ đúng vào catalog 'lakehouse' và schema 'gold' đang được quản lý bởi Nessie/dbt
        self.catalog = os.getenv("TRINO_CATALOG", "lakehouse")
        self.schema = os.getenv("TRINO_SCHEMA", "gold")
        
        # Guardrail Configurations
        self.max_limit = int(os.getenv("TRINO_MAX_LIMIT", "1000"))
        self.timeout = int(os.getenv("TRINO_TIMEOUT", "30")) # Timeout 30s chống treo hệ thống

    def _validate_and_rewrite(self, sql: str) -> str:
        """
        Bóc tách và kiểm tra an toàn câu lệnh SQL bằng sqlglot (AST).
        """
        try:
            # 1. Parse chuỗi SQL thành cây AST theo Dialect của Trino
            ast = sqlglot.parse_one(sql, read="trino")
        except sqlglot.errors.ParseError as e:
            logger.error(f"Lỗi phân tích cú pháp SQL: {e}")
            raise ValueError(f"Câu lệnh SQL do LLM sinh ra không hợp lệ về mặt cú pháp.")

        # 2. Guardrail 1: Chặn DML/DDL (Chỉ cho phép DQL - SELECT/WITH)
        # Bất kỳ lệnh nào như DROP, DELETE, ALTER, INSERT sẽ không phải là dạng exp.Select hoặc Subquery
        if not isinstance(ast, (exp.Select, exp.Subquery)):
            logger.critical(f"[SECURITY ALERT] AI Agent định thực thi lệnh cấm: {sql}")
            raise SecurityGuardrailException(
                "Nghiêm cấm thực thi! Hệ thống chỉ hỗ trợ truy vấn ĐỌC (SELECT)."
            )

        # 3. Guardrail 2: Tự động tiêm LIMIT nếu bị thiếu
        # Tránh truy vấn hàng tỷ dòng gây Out-Of-Memory (OOM) cho cluster Trino
        if not ast.args.get("limit"):
            logger.info(f"Câu lệnh thiếu LIMIT, tự động chèn LIMIT {self.max_limit}.")
            ast = ast.limit(self.max_limit)

        # Trả về câu lệnh SQL đã được làm sạch và chuẩn hóa lại theo dialect Trino
        return ast.sql(dialect="trino")

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
        Thực thi câu SQL an toàn và trả về Pandas DataFrame để dễ dàng xử lý.
        """
        # Bắt buộc đi qua Guardrail trước khi chạm vào Database
        safe_sql = self._validate_and_rewrite(sql)
        logger.info(f"Tiến hành chạy SQL an toàn trên Trino:\n{safe_sql}")

        # Mở kết nối Trino
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme='http',
            request_timeout=self.timeout
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(safe_sql)
                rows = cursor.fetchall()
                
                # Lấy tên cột từ cursor description
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Chuyển đổi sang DataFrame
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Truy vấn thành công! Lấy về {len(df)} dòng dữ liệu.")
                return df

        except TrinoUserError as e:
            # Bắt riêng lỗi từ User (như lỗi Access Denied, Not Found) để truyền thông báo rõ ràng cho Agent
            logger.error(f"[TRINO USER ERROR] Lỗi phân quyền hoặc cú pháp: {e.message}")
            raise ValueError(f"Lỗi truy cập Trino: {e.message}")
        except (TrinoExternalError, TrinoQueryError) as e:
            logger.error(f"[TRINO ENGINE ERROR] Lỗi khi thực thi: {e}")
            raise ValueError(f"Lỗi hệ thống từ Trino: {e}")
        finally:
            # Đảm bảo connection luôn được đóng dù xảy ra lỗi
            conn.close()

# Khởi tạo instance dùng chung (Singleton)
trino_engine = TrinoExecutor()