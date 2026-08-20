import yaml
import os
import glob

# 1. Khai báo đường dẫn tương đối dựa trên cấu trúc thư mục của em
dataset_file = os.path.join("datasets", "Trino_Lakehouse", "enriched_consumer_complaints_1.yaml")
charts_folder = "charts"

print("Đang đọc dữ liệu Dataset...")
# 2. Đọc file Dataset để lấy danh sách cột hợp lệ
try:
    with open(dataset_file, "r", encoding="utf-8") as f:
        dataset_data = yaml.safe_load(f)
    
    # Lấy danh sách tên cột
    valid_columns = [col["column_name"] for col in dataset_data.get("columns", [])]
    print(f"-> Thành công! Tìm thấy {len(valid_columns)} cột hợp lệ trong Dataset.\n")
except FileNotFoundError:
    print(f"❌ Lỗi: Không tìm thấy file Dataset tại đường dẫn: {dataset_file}")
    print("Vui lòng kiểm tra lại xem script đã đặt đúng ở thư mục consumer_complaints_export chưa.")
    exit()

print("-" * 50)
print("ĐANG QUÉT LỖI TRONG CÁC FILE CHART...\n")

# 3. Quét tất cả các file .yaml trong thư mục charts/
chart_files = glob.glob(os.path.join(charts_folder, "*.yaml"))

if not chart_files:
    print(f"⚠️ Không tìm thấy file chart nào trong thư mục {charts_folder}/")

error_count = 0

for file_path in chart_files:
    with open(file_path, "r", encoding="utf-8") as f:
        chart_data = yaml.safe_load(f)
        
        # Lấy các cột được dùng trong groupby của chart
        params = chart_data.get("params", {})
        if not isinstance(params, dict):
            continue
            
        groupby_cols = params.get("groupby", [])
        
        # Đề phòng trường hợp groupby là chuỗi thay vì list
        if isinstance(groupby_cols, str):
            groupby_cols = [groupby_cols]
        elif groupby_cols is None:
            groupby_cols = []
            
        # Đối chiếu cột
        for col in groupby_cols:
            if col not in valid_columns:
                slice_name = chart_data.get('slice_name', 'Không rõ tên')
                file_name = os.path.basename(file_path)
                print(f"❌ Chart: [{slice_name}]")
                print(f"   - File: {file_name}")
                print(f"   - Lỗi: Cột '{col}' không tồn tại trong Dataset.\n")
                error_count += 1

print("-" * 50)
if error_count == 0:
    print("✅ Tuyệt vời! Tất cả các chart đều hợp lệ, không bị lệch cột nào.")
else:
    print(f"⚠️ Phát hiện tổng cộng {error_count} lỗi cần sửa trước khi import!")