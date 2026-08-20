import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any

class NessieAPIClient:
    def __init__(self, host: str, port: int, api_version: str = "v1"):
        """Khởi tạo Nessie Client với cơ chế Retry chuẩn Production."""
        self.base_url = f"http://{host}:{port}/api/{api_version}"
        self.logger = logging.getLogger(__name__)
        
        # Thiết lập Connection Pooling & Tự động Retry
        self.session = requests.Session()
        retries = Retry(
            total=3,                # Thử lại tối đa 3 lần nếu mạng lỗi
            backoff_factor=2,       # Chờ 2s, 4s, 8s giữa các lần thử
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get_reference(self, ref_name: str) -> Dict[str, Any]:
        """Lấy thông tin nhánh và mã hash hiện tại."""
        url = f"{self.base_url}/trees/tree/{ref_name}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def create_branch(self, branch_name: str, source_ref: str = "main") -> str:
        """Tạo nhánh mới (Idempotent - Chạy lại nhiều lần không lỗi)."""
        source_info = self.get_reference(source_ref)
        source_hash = source_info["hash"]
        
        url = f"{self.base_url}/trees/tree"
        payload = {"type": "BRANCH", "name": branch_name, "hash": source_hash}
        response = self.session.post(url, json=payload)
        
        if response.status_code == 409:
            self.logger.info(f"🌿 Nhánh '{branch_name}' đã tồn tại. Tái sử dụng an toàn.")
            return self.get_reference(branch_name)["hash"]
            
        response.raise_for_status()
        self.logger.info(f"🌿 Đã tạo nhánh '{branch_name}' (Hash: {source_hash[:8]})")
        return source_hash

    def merge_branch(self, from_branch: str, to_branch: str = "main") -> None:
        """Merge nhánh WAP vào Production có kiểm tra Conflict."""
        to_info = self.get_reference(to_branch)
        expected_hash = to_info["hash"]
        from_info = self.get_reference(from_branch)
        from_hash = from_info["hash"]
        
        url = f"{self.base_url}/trees/branch/{to_branch}/merge?expectedHash={expected_hash}"
        payload = {"fromHash": from_hash, "fromRefName": from_branch}
        
        self.logger.info(f"🔄 Đang Merge '{from_branch}' -> '{to_branch}'...")
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        self.logger.info("✅ Merge thành công! Dữ liệu đã lên Production.")

    def drop_branch(self, branch_name: str) -> None:
        """Xóa nhánh an toàn."""
        try:
            branch_info = self.get_reference(branch_name)
            expected_hash = branch_info["hash"]
            url = f"{self.base_url}/trees/branch/{branch_name}?expectedHash={expected_hash}"
            response = self.session.delete(url)
            response.raise_for_status()
            self.logger.info(f"🗑️ Đã dọn dẹp nhánh tạm '{branch_name}'.")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"⚠️ Nhánh '{branch_name}' không tồn tại. Bỏ qua.")
            else:
                raise