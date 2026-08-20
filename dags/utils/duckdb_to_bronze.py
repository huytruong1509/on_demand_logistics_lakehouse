import logging
from typing import Dict, List, Tuple, Literal
import pyarrow as pa
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo, AlwaysTrue

logger = logging.getLogger(__name__)

class DuckDBIcebergLoader:
    def __init__(
        self, 
        s3_endpoint: str, 
        s3_access_key: str, 
        s3_secret_key: str, 
        nessie_uri: str,
        catalog_name: str = "lakehouse",
        s3_region: str = "us-east-1",
        branch: str = "main"
    ) -> None:
        self.pyiceberg_s3_endpoint = s3_endpoint 
        self.duckdb_s3_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")
        self.catalog_name = catalog_name
        self.branch = branch
        
        # 1. KHỞI TẠO PYICEBERG CATALOG
        self.catalog = load_catalog(
            self.catalog_name,
            **{
                "type": "rest",
                "uri": nessie_uri,
                "s3.endpoint": self.pyiceberg_s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
                "s3.region": s3_region,
                "s3.path-style-access": "true",
                "ref": self.branch
            }
        )

        # 2. KHỞI TẠO DUCKDB CONNECTION
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC';")
        
        # Cấu hình S3 cho DuckDB
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute(f"""
            CREATE SECRET s3_storage (
                TYPE S3,
                KEY_ID '{s3_access_key}',
                SECRET '{s3_secret_key}',
                ENDPOINT '{self.duckdb_s3_endpoint}',
                REGION '{s3_region}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)

        # Chỉ cần load Iceberg Extension để dùng hàm iceberg_scan()
        self.con.execute("INSTALL iceberg; LOAD iceberg;")
        
        logger.info(f"Đã khởi tạo DuckDB và PyIceberg Catalog trên nhánh '{self.branch}'.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type:
            logger.error(f"Pipeline dừng đột ngột do lỗi: {exc_val}")

    def _generate_dynamic_sql(self, target_path: str, bronze_columns: Dict[str, str], run_id: str, run_date: str, source_system: str) -> str:
        select_clauses = []
        json_schema_def = []
        
        for col_name, data_type in bronze_columns.items():
            # Xử lý tương thích kiểu dữ liệu: DuckDB dùng TIMESTAMPTZ thay cho TIMESTAMP(6) WITH TIME ZONE của Iceberg/Trino
            duckdb_type = data_type
            if "TIMESTAMP" in data_type.upper() and "TIME ZONE" in data_type.upper():
                duckdb_type = "TIMESTAMPTZ"
                
            if col_name == '_ingest_time':
                select_clauses.append("current_timestamp AS _ingest_time")
            elif col_name == '_run_id':
                select_clauses.append(f"'{run_id}' AS _run_id")
            elif col_name == '_source_system':
                select_clauses.append(f"'{source_system}' AS _source_system")
            elif col_name == '_run_date':
                select_clauses.append(f"CAST('{run_date}' AS DATE) AS _run_date")
            else:
                # Ép kiểu động dựa vào duckdb_type
                select_clauses.append(f"CAST(src.{col_name} AS {duckdb_type}) AS {col_name}")
                # Best Practice: Đọc JSON raw dưới dạng VARCHAR để tránh vỡ pipeline khi có bad format, ép kiểu sẽ diễn ra tại khâu SELECT
                json_schema_def.append(f"'{col_name}': 'VARCHAR'")

        dynamic_select = ",\n                ".join(select_clauses)
        dynamic_schema_str = ", ".join(json_schema_def)

        return f"""
            SELECT {dynamic_select}
            FROM read_json('{target_path}', format= 'newline_delimited', ignore_errors=true, columns={{{dynamic_schema_str}}}) AS src
        """

    def _execute_pre_ingestion_cleanup(self, iceberg_table, run_id: str, run_date: str, write_mode: str):
        """Hàm private xử lý việc dọn dẹp dữ liệu trước khi ghi (Hỗ trợ Incremental & Full Load)"""
        if write_mode == 'overwrite_run_id':
            logger.info(f"🧹 [Incremental] Đang dọn dẹp dữ liệu cũ của _run_id = '{run_id}' trên nhánh '{self.branch}'...")
            iceberg_table.delete(EqualTo("_run_id", run_id))
            logger.info("✅ Dọn dẹp Incremental hoàn tất.")
            
        elif write_mode == 'overwrite':
            logger.info(f"💣 [Full Load] Đang xóa sạch TOÀN BỘ dữ liệu trong bảng...")
            # Xóa toàn bộ row nhưng vẫn giữ nguyên cấu trúc Schema của Iceberg
            iceberg_table.delete(AlwaysTrue())
            logger.info("✅ Xóa trắng bảng hoàn tất. Chuẩn bị ghi dữ liệu Full Load.")

        elif write_mode == 'overwrite_run_date':
            logger.info(f"🧹 [Idempotent] Đang dọn dẹp Partition của _run_date = '{run_date}' trên nhánh '{self.branch}'...")
            iceberg_table.delete(EqualTo("_run_date", run_date))
            logger.info("✅ Dọn dẹp an toàn theo Logical Date hoàn tất.")
            
        else:
            logger.info(f"⏩ [Append] Bỏ qua bước dọn dẹp, tiến hành cộng dồn dữ liệu.")

    def load_incremental(
        self, bucket: str, prefix: str, target_schema: str, target_table: str, 
        run_id: str, run_date: str, bronze_columns: Dict[str, str], source_system: str,
        write_mode: Literal['append', 'overwrite_run_id', 'overwrite', 'overwrite_run_date'] = 'append' # Bổ sung 'overwrite' và 'overwrite_run_date'
    ) -> int:
        target_path = f"s3://{bucket}/{prefix}/run_date={run_date}/*.json"
        query = self._generate_dynamic_sql(target_path, bronze_columns, run_id, run_date, source_system)
        table_identifier = f"{target_schema}.{target_table}"
        
        try:
            iceberg_table = self.catalog.load_table(table_identifier)
            
            # Kích hoạt logic dọn dẹp động
            self._execute_pre_ingestion_cleanup(iceberg_table, run_id, run_date, write_mode)

            try:
                arrow_table = self.con.execute(query).to_arrow_table()
            except AttributeError:
                arrow_table = self.con.execute(query).fetch_arrow_table()
            
            total_records = arrow_table.num_rows
            if total_records > 0:
                iceberg_table.append(arrow_table)
                logger.info(f"Hoàn tất (Batch): {total_records} dòng vào {table_identifier}.")
            return total_records
                    
        except duckdb.IOException:
            return 0
        except Exception as e:
            logger.error(f"Lỗi hệ thống khi nạp Batch ETL: {e}")
            raise

    def load_incremental_stream(
        self, bucket: str, prefix: str, target_schema: str, target_table: str, 
        run_id: str, run_date: str, bronze_columns: Dict[str, str], source_system: str, chunk_size: int = 250000,
        write_mode: Literal['append', 'overwrite_run_id', 'overwrite', 'overwrite_run_date'] = 'append' # Bổ sung 'overwrite' và 'overwrite_run_date'
    ) -> int:
        target_path = f"s3://{bucket}/{prefix}/run_date={run_date}/*.json"
        query = self._generate_dynamic_sql(target_path, bronze_columns, run_id, run_date, source_system)
        table_identifier = f"{target_schema}.{target_table}"
        
        logger.info(f"Bắt đầu chế độ STREAMING cho ngày: {run_date} (Chunk Size: {chunk_size})")
        
        try:
            iceberg_table = self.catalog.load_table(table_identifier)
            
            # Kích hoạt logic dọn dẹp động
            self._execute_pre_ingestion_cleanup(iceberg_table, run_id, run_date, write_mode)
            
            query_result = self.con.execute(query)
            
            try:
                arrow_reader = query_result.fetch_record_batch(chunk_size)
            except duckdb.IOException as e:
                logger.warning(f"File JSON không tồn tại hoặc rỗng: {e}")
                return 0

            total_inserted_rows = 0
            chunk_count = 0

            for batch in arrow_reader:
                chunk_table = pa.Table.from_batches([batch])
                rows_in_chunk = chunk_table.num_rows
                
                if rows_in_chunk > 0:
                    iceberg_table.append(chunk_table)
                    total_inserted_rows += rows_in_chunk
                    chunk_count += 1
                    logger.info(f"-> Đã ghi thành công chunk #{chunk_count} ({rows_in_chunk:,} dòng) vào Iceberg.")

            logger.info(f"✅ Hoàn tất (Stream): Đã nạp {total_inserted_rows:,} dòng vào {table_identifier}.")
            return total_inserted_rows
            
        except NoSuchTableError:
            logger.error(f"Bảng đích {table_identifier} chưa được tạo trên Catalog.")
            raise
        except Exception as e:
            logger.error(f"Lỗi hệ thống khi Streaming ETL: {e}")
            raise

    def get_metadata_location(self, target_schema: str, target_table: str) -> str:
        table_identifier = f"{target_schema}.{target_table}"
        iceberg_table = self.catalog.load_table(table_identifier)
        return iceberg_table.metadata_location

    def execute_audit_query(self, query: str) -> List[Tuple]:
        logger.info(f"Đang thực thi Audit Query qua DuckDB: {query}")
        return self.con.execute(query).fetchall()

    def close(self):
        if hasattr(self, 'con'):
            self.con.close()
            logger.info("Đã đóng kết nối DuckDB an toàn.")