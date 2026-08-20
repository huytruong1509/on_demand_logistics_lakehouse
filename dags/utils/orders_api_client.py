"""
Logistics Orders API Client

This module provides a client for interacting with the internal Orders API.
Supports both Batch (Full Load) and Cursor-based Incremental extracts.
"""

import logging
import json
from typing import Any, Dict, Iterator, List, Optional, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class OrdersAPIClient:
    """Client for accessing the Logistics Orders API."""

    DEFAULT_TIMEOUT = 120
    MAX_RETRIES = 3

    def __init__(self, base_url: str = "http://data_source:8005/api/v1", timeout: int = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "DataLakehouseProject/2.0 (Logistics Pipeline)",
            "Accept": "application/json"
        })

        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _call_api(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        try:
            logger.debug(f"Đang gửi GET Request tới {url} với params: {params}")
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Lỗi kết nối mạng/HTTP khi gọi API: {e}")
            raise

    def get_batch_orders(self, limit: int = 10000) -> Iterator[List[Dict[str, Any]]]:
        last_id = None 
        total_records = 0
        
        logger.info("🚀 Khởi tạo luồng kéo TOÀN BỘ dữ liệu Batch (Keyset Stream)")
        
        while True:
            params: Dict[str, Any] = {"limit": limit}
            if last_id is not None:
                params["last_id"] = last_id
                
            response = self._call_api("orders/batch", params)
            data = response.get("data", [])
            
            if not data:
                break
                
            yield data
            
            total_records += len(data)
            logger.info(f"  ➜ Đã kéo được {total_records:,} records...")
            
            if len(data) < limit:
                logger.info(f"Đã chạm đáy (Trang có {len(data)} < limit {limit}). Dừng phân trang an toàn.")
                break
                
            next_cursor = response.get("next_cursor")
            
            if next_cursor is not None:
                if next_cursor == last_id:
                    logger.error("🚨 CRITICAL: next_cursor bị kẹt không đổi. Ngắt vòng lặp!")
                    break
                last_id = next_cursor
            else:
                last_record = data[-1]
                last_id = last_record.get("_db_id") or last_record.get("id")
                
                if last_id is None:
                    logger.error("🚨 CRITICAL: Backend thiếu next_cursor và record không có _db_id!")
                    break
                
        logger.info(f"✅ HOÀN TẤT: Đã lấy tổng cộng {total_records:,} bản ghi từ Batch API.")
    
    def get_incremental_stream(self, since: float, limit: int = 2000) -> Iterator[List[Dict[str, Any]]]:
        current_cursor = since
        records_yielded = 0
        
        logger.info(f"🚀 BẮT ĐẦU: Kéo dữ liệu Incremental từ mốc thời gian (since): {current_cursor}")

        while True:
            params: Dict[str, Any] = {
                "since": current_cursor,
                "limit": limit
            }
            
            response = self._call_api("orders/incremental", params)
            data = response.get("data", [])
            next_cursor = response.get("next_cursor")
            
            if not data:
                logger.info("Không còn dữ liệu mới trên API. Hoàn tất chu kỳ Incremental.")
                break

            yield data
            
            records_yielded += len(data)
            logger.info(f"  ➜ Đã kéo được {records_yielded:,} records mới...")
            
            if next_cursor == current_cursor or not next_cursor:
                break
            
            current_cursor = next_cursor

    def get_safe_incremental_stream(self, expected_schema_keys: Set[str], since: float, limit: int = 2000) -> Iterator[List[Dict[str, Any]]]:
        logger.info("🛡️ Khởi động lưới lọc Rescued Data cho Incremental Load")
        
        stream = self.get_incremental_stream(since=since, limit=limit)

        for chunk in stream:
            processed_chunk = []
            for record in chunk:
                processed_record = {}
                unknown_fields = {}
                for key, value in record.items():
                    if value is None:
                        continue
                    if key in expected_schema_keys:
                        if isinstance(value, (dict, list)):
                            processed_record[key] = json.dumps(value, ensure_ascii=False)
                        else:
                            processed_record[key] = str(value)
                    else:
                        unknown_fields[key] = value

                if unknown_fields:
                    processed_record["_rescued_data"] = json.dumps(unknown_fields, ensure_ascii=False)

                processed_chunk.append(processed_record)
            
            yield processed_chunk

    def close(self):
        if self.session:
            self.session.close()
            logger.info("Orders API client session closed")