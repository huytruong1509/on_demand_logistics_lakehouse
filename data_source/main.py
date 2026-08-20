import math
import random
import string
import time
import sqlite3
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import FastAPI, Query, HTTPException
from contextlib import asynccontextmanager

# Import osmnx để xử lý tọa độ bản đồ
import osmnx as ox

# ==========================================
# CẤU HÌNH DATABASE & MÔI TRƯỜNG
# ==========================================
TOTAL_ROWS = 200000
DB_FILE = "/app/db/mock_data.db"
LOCATIONS_FILE = "/app/db/osmnx_locations.json"
BATCH_SIZE = 10000

# ==========================================
# 1. TỪ ĐIỂN CẤU HÌNH & DATA MẪU
# ==========================================

START_TIMESTAMP_2025 = datetime(2025, 1, 1).timestamp()
END_TIMESTAMP_2025 = datetime(2025, 12, 31, 23, 59, 59).timestamp()

CITIES = ["SGN", "HAN"]
SERVICES = {
    "SGN": ["SGN-BIKE-SIEU-TOC", "SGN-BIKE-TIET-KIEM", "SGN-BIKE-2H", "SGN-BIKE-4H"],
    "HAN": ["HAN-BIKE-SIEU-TOC", "HAN-BIKE-TIET-KIEM", "HAN-BIKE-2H", "HAN-BIKE-4H"]
}

BIKE_SERVICE_RULES = {
    "SIEU-TOC": {"max_stops": 10, "stop_fee_per_point": 5400, "max_cod": 10_000_000},
    "TIET-KIEM": {"max_stops": 1, "stop_fee_per_point": 0, "max_cod": 10_000_000},
    "2H": {"max_stops": 1, "stop_fee_per_point": 0, "max_cod": 1_000_000},
    "4H": {"max_stops": 1, "stop_fee_per_point": 0, "max_cod": 1_000_000}
}

PARTNERS = [
    "kiotviet", "sapo", "haravan", "shopee_food", "tiktokshop", 
    "lazada", "thecoffeehouse", "highlandscoffee", None
]

PAYMENT_METHODS = ["CASH", "CASH_BY_RECIPIENT", "BALANCE"]

PROMO_POOL = [
    None,
    {"code": "AHA10K", "type": "FIXED", "value": 10000},
    {"code": "AHADONGGIA15K", "type": "FIXED", "value": 15000},
    {"code": "AHAGIAOTHANH", "type": "PERCENT", "value": 0.20, "max_discount": 20000},
    {"code": "FREESHIP2025", "type": "FIXED", "value": 5000}
]

# Sẽ được nạp động từ OSMnx
LOCATIONS = {"SGN": [], "HAN": []}

APT_NUMBERS = [None]

REMARKS_POOL = [
    "Gọi điện trước khi đến 5 phút",
    "Gửi bảo vệ / Lễ tân tầng trệt",
    "Hàng dễ vỡ, xin nhẹ tay",
    "Khách trả tiền phí ship + COD",
    "Hàng quần áo, cho khách xem hàng không thử",
    "Đến đúng giờ, khách cần gấp",
    "Quán cà phê đông, đến gọi chủ shop ra giao"
]

DRIVER_CANCEL_REASONS = [
    "supplier_sender_doesnt_pickup_phone", "supplier_unable_to_contact_sender",
    "supplier_receipient_doesnt_pickup_phone", "supplier_unable_to_contact_receipient",
    "supplier_pickup_is_so_far", "supplier_dropoff_is_so_far",
    "supplier_wrong_pickup", "supplier_wrong_dropoff",
    "supplier_pickup_is_changed", "supplier_dropoff_is_changed",
    "supplier_delivery_time_has_changed", "supplier_sender_cancel",
    "supplier_receipient_cancel", "supplier_bulky_package",
    "supplier_wait_for_pickup_too_long", "supplier_not_enough_advance_money",
    "supplier_broken_vehicle", "supplier_driver_not_available", "supplier_orther"
]

USER_CANCEL_REASONS = [
    "user_incorrect_pickup", "user_incorrect_dropoff", "user_not_input_promotion",
    "user_incorrect_time_delivery", "user_incorrect_recipient", "user_no_driver_accept",
    "user_pickup_is_so_far_from_driver", "user_driver_asked_cancel",
    "user_driver_takes_too_time_to_pickup", "user_no_item", "user_user_change_driver",
    "user_user_change_vehicle", "user_user_use_another_app", "user_incorrect_item_description",
    "user_incorrect_special_requests", "user_incorrect_payment_method",
    "user_package_too_large", "user_driver_collect_extra_fee",
    "user_driver_behavior", "user_incorrect_vehicle", "user_user_wrong"
]

FAIL_REASONS = [
    "supplier_receipient_doesnt_pickup_phone", "supplier_unable_to_contact_recipient",
    "supplier_incorrect_recipient_phone_number", "supplier_recipient_not_show_up",
    "supplier_recipient_reschedules_another_day", "supplier_recipient_reschedules_another_time_inday",
    "supplier_recipient_changes_delivery_address", "supplier_sender_changes_delivery_address",
    "supplier_recipient_provides_incorrect_address", "supplier_package_inspection_is_not_allowed",
    "supplier_dropoff_is_changed", "supplier_package_not_match_request",
    "supplier_incorrect_cod_amount", "supplier_recipient_changes_decision",
    "supplier_duplicated_or_scam_order", "supplier_broken_package",
    "supplier_lost_package", "supplier_recipient_has_no_cash_available",
    "supplier_language_diffrences", "supplier_sender_ask_to_return_the_package"
]

USERS = [{"user_id": f"849{random.randint(10000000, 99999999)}", "user_name": name} for name in ["Thảo Vy", "Minh Trần", "Anh Nguyễn", "Bảo Bảo", "Đức Anh"]]
SUPPLIERS = [{"supplier_id": f"849{random.randint(10000000, 99999999)}", "supplier_name": name} for name in ["Hiếu Nguyễn", "Tuấn Lê", "Đức Phạm", "Hoàng Anh", "Văn Hùng"]]

USER_ORDER_COUNTERS: Dict[str, int] = {}
# ==========================================
# 2. HÀM LẤY TỌA ĐỘ TỪ OSMNX (CACHE CƠ CHẾ)
# ==========================================

def fetch_real_locations_osmnx() -> Dict[str, list]:
    print("[*] Đang tải dữ liệu mạng lưới đường bộ và POI từ OpenStreetMap qua OSMnx (sẽ mất khoảng 1-2 phút)...")
    ox.settings.use_cache = True
    ox.settings.log_console = False
    
    # Lấy các quận nội thành đại diện để tăng tốc độ tải
    queries = {
        "SGN": ["District 1, Ho Chi Minh City, Vietnam", "District 10, Ho Chi Minh City, Vietnam"],
        "HAN": ["Hoan Kiem District, Hanoi, Vietnam", "Dong Da District, Hanoi, Vietnam"]
    }
    
    result_locs = {"SGN": [], "HAN": []}
    
    for city, places in queries.items():
        for place in places:
            try:
                print(f"    -> Đang xử lý: {place}")
                # 1. Lấy đồ thị đường dành cho xe cộ (drive)
                G = ox.graph_from_place(place, network_type='drive')
                
                # Trích xuất cả nodes (tọa độ vật lý) và edges (dữ liệu đường)
                gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)
                
                # Trích xuất tập tên đường thực tế có ở khu vực này
                if 'name' in gdf_edges.columns:
                    # Dữ liệu name có thể là string hoặc list string, cần làm sạch
                    street_names = gdf_edges['name'].dropna()
                    street_names = street_names.apply(lambda x: x[0] if isinstance(x, list) else x)
                    local_streets = street_names.unique().tolist()
                else:
                    local_streets = ["Đường không tên"]
                
                # 2. Lấy danh sách tòa nhà/tiện ích thực tế tại khu vực này
                try:
                    tags = {"building": True, "amenity": True, "shop": True}
                    gdf_pois = ox.features_from_place(place, tags=tags)
                    
                    if 'name' in gdf_pois.columns:
                        local_buildings = gdf_pois['name'].dropna().unique().tolist()
                    else:
                        local_buildings = []
                except Exception as e:
                    print(f"       [!] Không thể tải POI cho {place}: {e}")
                    local_buildings = []
                
                if local_buildings:
                    local_buildings.extend([None] * len(local_buildings))
                else:
                    local_buildings = [None]
                
                # Sample ngẫu nhiên 100 điểm trên đường mỗi khu vực
                sample_size = min(100, len(gdf_nodes))
                sampled = gdf_nodes.sample(sample_size)
                
                short_place_name = place.split(',')[0]
                for idx, row in sampled.iterrows():
                    # Tạo số nhà ngẫu nhiên và mix với tên đường thực tế
                    house_number = random.randint(1, 999)
                    street = random.choice(local_streets)
                    
                    result_locs[city].append({
                        "address": f"{house_number} {street}, {short_place_name}",
                        "lat": float(row['y']),
                        "lng": float(row['x']),
                        "building": random.choice(local_buildings)  # Gán POI logic thay vì hardcode
                    })
            except Exception as e:
                print(f"    [!] Lỗi khi tải {place}: {e}")
                
    # Lưu vào file JSON để cache lại cho các lần khởi động sau
    with open(LOCATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(result_locs, f, ensure_ascii=False, indent=2)
        
    print("[*] Đã tải và lưu trữ thành công các tọa độ và POI thực tế.")
    return result_locs

def init_locations():
    global LOCATIONS
    if os.path.exists(LOCATIONS_FILE):
        print("[*] Đã tìm thấy cache tọa độ OSMnx. Đang nạp vào bộ nhớ...")
        with open(LOCATIONS_FILE, 'r', encoding='utf-8') as f:
            LOCATIONS = json.load(f)
    else:
        LOCATIONS = fetch_real_locations_osmnx()
        
    # Fallback dự phòng trường hợp OSMnx bị lỗi mạng
    if not LOCATIONS["SGN"]:
        LOCATIONS["SGN"] = [{"address": "220 Điện Biên Phủ, Quận 3", "lat": 10.7852, "lng": 106.6908, "building": None}]
    if not LOCATIONS["HAN"]:
        LOCATIONS["HAN"] = [{"address": "54 Nguyễn Chí Thanh, Đống Đa", "lat": 21.0227, "lng": 105.8094, "building": None}]

# ==========================================
# 3. LOGIC TÍNH PHÍ VÀ KHOẢNG CÁCH (GIỮ NGUYÊN)
# ==========================================

def calculate_bike_fees(service_type: str, distance_km: float, num_stops: int, cod_amount: int, is_return_to_pickup: bool = False) -> Dict[str, int]:
    distance_fee = stop_fee = cod_fee = 0
    
    if service_type == "SIEU-TOC":
        if distance_km <= 2.0: distance_fee = 15709
        elif distance_km <= 3.0: distance_fee = 19636
        else: distance_fee = 19636 + int((distance_km - 3.0) * 5400)
    elif service_type == "TIET-KIEM":
        if distance_km <= 2.0: distance_fee = 13745
        elif distance_km <= 3.0: distance_fee = 17673
        else: distance_fee = 17673 + int((distance_km - 3.0) * 4909)
    elif service_type == "2H":
        if distance_km <= 4.0: distance_fee = 19636
        else: distance_fee = 19636 + int((distance_km - 4.0) * 4320)
    elif service_type == "4H":
        if distance_km < 10.0: distance_fee = 24000
        elif distance_km < 20.0: distance_fee = 32000
        else: distance_fee = 50000

    if service_type == "SIEU-TOC" and num_stops > 1:
        stop_fee = (num_stops - 1) * 5400

    if service_type in ["SIEU-TOC", "TIET-KIEM"]:
        if 500_000 <= cod_amount <= 5_000_000: cod_fee = int(cod_amount * 0.006)
        elif cod_amount > 5_000_000: cod_fee = int(cod_amount * 0.0088)
    elif service_type == "2H" and cod_amount >= 500_000:
        cod_fee = int(min(cod_amount, 1_000_000) * 0.006)

    return_fee = int(distance_fee * 0.8) if (is_return_to_pickup and service_type in ["SIEU-TOC", "TIET-KIEM"]) else 0

    return {
        "distance_fee": distance_fee,
        "stop_fee": stop_fee,
        "cod_fee": cod_fee,
        "return_fee": return_fee
    }

def calculate_logical_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0 
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2)**2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * (math.sin(dlon / 2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return (R * c) * 1.41 # Nhân hệ số tortuosity (độ cong giao thông đô thị)

# ==========================================
# 4. LOGIC SINH DỮ LIỆU ĐƠN HÀNG
# ==========================================

def simulate_order(base_timestamp: Optional[float] = None) -> Dict[str, Any]:
    city_id = random.choice(CITIES)
    service_id = random.choice(SERVICES[city_id])
    service_type = service_id.split("-")[-1] if ("2H" in service_id or "4H" in service_id) else ("TIET-KIEM" if "TIET-KIEM" in service_id else "SIEU-TOC")

    create_time = order_time = accept_time = board_time = pickup_time = None
    complete_time = cancel_time = return_time = idle_until = None
    accept_lat = accept_lng = accept_distance = accept_duration = None
    
    create_time = (base_timestamp + random.uniform(1, 3600)) if base_timestamp else random.uniform(START_TIMESTAMP_2025, END_TIMESTAMP_2025)
    is_scheduled = random.random() < 0.2
    order_time = create_time + random.randint(1800, 7200) if is_scheduled else create_time
    
    scenario = random.choices(["SUCCESS", "RETURNED", "CANCEL_AUTO", "CANCEL_AFTER_ACCEPT"], weights=[0.8542, 0.0315, 0.0428, 0.0715])[0]
    
    if scenario in ["SUCCESS", "RETURNED"]:
        status = "COMPLETED"
        sub_status = "RETURNED" if scenario == "RETURNED" else None
    else:
        status = "CANCELLED"
        sub_status = None

    user = random.choice(USERS)
    user_id = user["user_id"]
    partner = random.choices(PARTNERS, weights=[0.1, 0.1, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.5])[0]
    
    # Lấy Pool tọa độ từ biến LOCATIONS (đã được load OSMnx)
    pool_locs = LOCATIONS[city_id]
    pickup_loc = random.choice(pool_locs)
    
    supplier = {"supplier_id": None, "supplier_name": None}
    cancel_by_user = None
    cancel_comment = None

    if scenario == "CANCEL_AUTO":
        cancel_time = order_time + random.randint(300, 1200)
        cancel_by_user = False
        cancel_comment = "Auto cancel, no driver accepted" 
    elif scenario == "CANCEL_AFTER_ACCEPT":
        supplier = random.choice(SUPPLIERS)
        accept_time = order_time + random.randint(15, 300)
        accept_lat, accept_lng = pickup_loc["lat"] + random.uniform(-0.005, 0.005), pickup_loc["lng"] + random.uniform(-0.005, 0.005)
        
        # Sửa lỗi: Tính accept_distance logic thực tế thay vì random
        calculated_dist = calculate_logical_distance(accept_lat, accept_lng, pickup_loc["lat"], pickup_loc["lng"])
        accept_distance = round(max(0.1, calculated_dist), 2)
        accept_duration = int(accept_distance * 120)
        
        has_boarded = random.choice([True, False])
        if has_boarded:
            upper_bound = max(30, int(accept_duration * 0.8))
            board_time = accept_time + random.randint(30, upper_bound)
            cancel_time = board_time + random.randint(30, 300)
        else:
            cancel_time = accept_time + random.randint(30, 300)

        cancel_actor = random.choices(["USER", "DRIVER"], weights=[0.5, 0.5])[0]
        cancel_by_user = (cancel_actor == "USER")
        cancel_comment = random.choice(USER_CANCEL_REASONS) if cancel_by_user else random.choice(DRIVER_CANCEL_REASONS)
                
    elif scenario in ["SUCCESS", "RETURNED"]:
        supplier = random.choice(SUPPLIERS)
        accept_time = order_time + random.randint(15, 300)
        accept_lat, accept_lng = pickup_loc["lat"] + random.uniform(-0.005, 0.005), pickup_loc["lng"] + random.uniform(-0.005, 0.005)
        
        # Sửa lỗi: Tính accept_distance logic thực tế thay vì random
        calculated_dist = calculate_logical_distance(accept_lat, accept_lng, pickup_loc["lat"], pickup_loc["lng"])
        accept_distance = round(max(0.1, calculated_dist), 2)
        accept_duration = int(accept_distance * 120)
        
        board_time = accept_time + int(accept_duration * 0.8)
        pickup_time = accept_time + accept_duration + random.randint(60, 300)

    path = [{
        "address": pickup_loc["address"], 
        "lat": pickup_loc["lat"], 
        "lng": pickup_loc["lng"],
        "name": user["user_name"], 
        "mobile": user["user_id"],
        "remarks": "Gọi người gửi lấy hàng",
        "building": pickup_loc.get("building"), # Lấy building logic
        "apt_number": random.choice(APT_NUMBERS),
        "status": None,
        "complete_time": None,
        "complete_lat": None,
        "complete_lng": None,
        "complete_comment": None,
        "image_url": None,
        "pod_info": None,
        "rating_by_receiver": None,
        "comment_by_receiver": None,
        "fail_time": None,
        "fail_lat": None,
        "fail_lng": None,
        "fail_comment": None,
        "redelivery_note": None
    }]
    
    max_allowed_stops = BIKE_SERVICE_RULES[service_type]["max_stops"]
    num_stops = random.randint(1, max_allowed_stops) if max_allowed_stops > 1 else 1
    
    # Sửa lỗi: Áp dụng thuật toán Greedy (Nearest Neighbor) giải bài toán TSP
    drop_candidates = [l for l in pool_locs if l["lat"] != pickup_loc["lat"] and l["lng"] != pickup_loc["lng"]]
    raw_drop_locs = random.sample(drop_candidates, k=min(num_stops, len(drop_candidates)))
    
    drop_locs = []
    tsp_lat, tsp_lng = pickup_loc["lat"], pickup_loc["lng"]
    
    while raw_drop_locs:
        nearest_drop = min(
            raw_drop_locs, 
            key=lambda d: calculate_logical_distance(tsp_lat, tsp_lng, d["lat"], d["lng"])
        )
        drop_locs.append(nearest_drop)
        raw_drop_locs.remove(nearest_drop)
        tsp_lat, tsp_lng = nearest_drop["lat"], nearest_drop["lng"]

    dropoff_time_cursor = pickup_time if pickup_time else order_time + 1200
    total_cod = 0
    total_distance = 0.0
    
    current_lat, current_lng = pickup_loc["lat"], pickup_loc["lng"]

    for idx, drop in enumerate(drop_locs, start=1):
        leg_dist = calculate_logical_distance(current_lat, current_lng, drop["lat"], drop["lng"])
        total_distance += leg_dist
        
        if pickup_time:
            dropoff_time_cursor += int(leg_dist * 120) + random.randint(120, 300)
            
        point_cod = random.choice([0, 100000, 200000, 500000]) if BIKE_SERVICE_RULES[service_type]["max_cod"] > 0 else 0
        total_cod += point_cod

        p = {
            "address": drop["address"], 
            "lat": drop["lat"], 
            "lng": drop["lng"],
            "name": f"Người nhận {idx}", 
            "mobile": f"849{random.randint(10000000, 99999999)}",
            "cod": point_cod, 
            "remarks": random.choice(REMARKS_POOL),
            "tracking_number": f"P-{random.randint(100000, 999999)}",
            "require_pod": True,
            "require_verification": random.choice([True, False]),
            "building": drop.get("building"), # Lấy building logic
            "apt_number": random.choice(APT_NUMBERS),
            "status": None,
            "complete_time": None,
            "complete_lat": None,
            "complete_lng": None,
            "complete_comment": None,
            "image_url": None,
            "pod_info": None,
            "rating_by_receiver": None,
            "comment_by_receiver": None,
            "fail_time": None,
            "fail_lat": None,
            "fail_lng": None,
            "fail_comment": None,
            "redelivery_note": None
        }
        
        if scenario == "SUCCESS":
            p["status"] = "COMPLETED"
            p["complete_time"] = round(dropoff_time_cursor, 5)
            p["complete_lat"] = drop["lat"]
            p["complete_lng"] = drop["lng"]
            p["complete_comment"] = "Giao thành công"
            p["image_url"] = "https://i.imgur.com/example.jpg"
            p["pod_info"] = "POD" + "".join(random.choices(string.digits, k=6))
            p["rating_by_receiver"] = random.choice([4, 5])
            p["comment_by_receiver"] = "Tài xế thân thiện"

        elif scenario == "RETURNED":
            if idx == len(drop_locs):  
                p["status"] = "FAILED"
                fail_ts = round(dropoff_time_cursor, 5)
                p["fail_time"] = fail_ts
                p["fail_lat"] = drop["lat"]
                p["fail_lng"] = drop["lng"]
                p["fail_comment"] = random.choice(FAIL_REASONS)
                p["redelivery_note"] = {
                    "from_time": fail_ts + 3600,
                    "to_time": fail_ts + 7200,
                    "address": pickup_loc["address"],
                    "lat": pickup_loc["lat"],
                    "lng": pickup_loc["lng"]
                }
            else:
                p["status"] = "COMPLETED"
                p["complete_time"] = round(dropoff_time_cursor, 5)
                p["complete_lat"] = drop["lat"]
                p["complete_lng"] = drop["lng"]
                p["complete_comment"] = "Giao thành công"
                p["image_url"] = "https://i.imgur.com/example.jpg"
                p["pod_info"] = "POD" + "".join(random.choices(string.digits, k=6))
                
        path.append(p)
        current_lat, current_lng = drop["lat"], drop["lng"]
        
    total_distance = round(max(0.1, total_distance), 2)

    if scenario == "SUCCESS":
        complete_time = dropoff_time_cursor + random.randint(5, 60)
    elif scenario == "RETURNED":
        complete_time = dropoff_time_cursor + random.randint(5, 60)
        return_time = complete_time + random.randint(600, 1800)

    fees = calculate_bike_fees(
        service_type=service_type, 
        distance_km=total_distance,
        num_stops=len(path) - 1, 
        cod_amount=total_cod,
        is_return_to_pickup=(sub_status == "RETURNED")
    )

    distance_fee = fees["distance_fee"]
    stop_fee = fees["stop_fee"]
    
    requests_array = []
    request_fee = 0
    
    if fees["cod_fee"] > 0:
        requests_array.append({"_id": f"{service_id}-COD", "price": fees["cod_fee"], "value": total_cod})
        request_fee += fees["cod_fee"]
    
    if fees["return_fee"] > 0:
        requests_array.append({"_id": f"{service_id}-RETURN", "price": fees["return_fee"]})
        request_fee += fees["return_fee"]
        
    if random.random() < 0.2:
        tip_price = 5000
        requests_array.append({"_id": f"{service_id}-TIP", "price": tip_price, "num": 1})
        request_fee += tip_price
        
    promo_info = random.choices(PROMO_POOL, weights=[0.6, 0.1, 0.1, 0.1, 0.1])[0]
    promo = promo_info["code"] if promo_info else None
    discount = 0

    if promo_info:
        if promo_info["type"] == "FIXED":
            discount = promo_info["value"]
        elif promo_info["type"] == "PERCENT":
            raw_discount = int((distance_fee + stop_fee + request_fee) * promo_info["value"])
            discount = min(raw_discount, promo_info["max_discount"])
    
    total_fee = max(0, distance_fee + stop_fee + request_fee - discount) 
    
    payment_method = random.choice(PAYMENT_METHODS)
    if payment_method == "CASH_BY_RECIPIENT" and len(path) > 2:
        payment_method = "CASH" 

    user_main_account = total_fee if payment_method == "BALANCE" else 0
    user_bonus_account = 0
    total_pay = max(0, total_fee - user_main_account - user_bonus_account)

    supplier_main_account = round(total_fee * 0.212, 1) if supplier["supplier_id"] else 0
    supplier_bonus_account = 0.0

    rating_by_user = random.choice([4, 5]) if status == "COMPLETED" else None
    comment_by_user = "Dịch vụ nhanh chóng" if rating_by_user else None
    rating_by_supplier = 5 if status == "COMPLETED" else None
    comment_by_supplier = "Khách hàng nhiệt tình" if rating_by_supplier else None

    is_remind = True if total_cod > 0 else False

    user_index = USER_ORDER_COUNTERS.get(user_id, 0)
    USER_ORDER_COUNTERS[user_id] = user_index + 1

    order_data = {
        "_id": "".join(random.choices(string.ascii_uppercase + string.digits, k=6)),
        "status": status,
        "sub_status": sub_status,
        "service_id": service_id,
        "city_id": city_id,
        
        "requests": requests_array, 
        
        "user_id": user["user_id"], 
        "user_name": user["user_name"],
        "supplier_id": supplier["supplier_id"], 
        "supplier_name": supplier["supplier_name"],
        "partner": partner,
        
        "create_time": create_time,
        "order_time": order_time,
        "idle_until": idle_until,
        
        "accept_time": accept_time,
        "accept_lat": accept_lat, 
        "accept_lng": accept_lng, 
        "accept_distance": accept_distance, 
        "accept_duration": accept_duration, 
        "board_time": board_time,
        "pickup_time": pickup_time,
        "cancel_time": cancel_time,
        "complete_time": complete_time,
        "return_time": return_time,
        
        "cancel_comment": cancel_comment,
        "cancel_by_user": cancel_by_user,
        
        "currency": "VND",
        "promo_code": promo,
        "payment_method": payment_method,
        "distance": total_distance,
        
        "distance_fee": distance_fee,
        "stop_fee": stop_fee,
        "request_fee": request_fee,
        "discount": discount,
        "total_fee": total_fee,
        
        "distance_price": distance_fee,
        "special_request_price": request_fee,
        "stoppoint_price": stop_fee,
        "voucher_discount": discount,
        "subtotal_price": distance_fee + stop_fee + request_fee,
        "total_price": total_fee,

        "user_main_account": user_main_account,
        "user_bonus_account": user_bonus_account, 
        "total_pay": total_pay, 
        "supplier_main_account": supplier_main_account,
        "supplier_bonus_account": supplier_bonus_account,

        "rating_by_user": rating_by_user,
        "comment_by_user": comment_by_user,
        "rating_by_supplier": rating_by_supplier, 
        "comment_by_supplier": comment_by_supplier, 
        
        "path": path,
        "remarks": random.choice(REMARKS_POOL),
        
        "remind": is_remind,
        "assigned_by": "auto",
        "index": 0,
        
        "from_location": {"type": "Point", "coordinates": [pickup_loc["lng"], pickup_loc["lat"]]}
    }
    
    return order_data


# ==========================================
# 5. DATA SEEDING & FASTAPI
# ==========================================

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mock_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                city_id TEXT,
                create_time REAL,
                raw_data TEXT
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_create_time ON mock_orders(create_time ASC)")
        conn.commit()

def seed_data():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mock_orders")
        current_count = cursor.fetchone()[0]
        
        missing = TOTAL_ROWS - current_count
        if missing > 0:
            print(f"[*] Database đang có {current_count}/{TOTAL_ROWS} đơn hàng.")
            print(f"[*] Tiến hành tạo thêm {missing} đơn hàng. Vui lòng đợi...")
            
            for i in range(0, missing, BATCH_SIZE):
                batch_limit = min(BATCH_SIZE, missing - i)
                batch_data = []
                
                for _ in range(batch_limit):
                    order = simulate_order()
                    batch_data.append((
                        order["_id"],
                        order["city_id"],
                        order["create_time"],
                        json.dumps(order)
                    ))
                
                cursor.executemany("""
                    INSERT INTO mock_orders (order_id, city_id, create_time, raw_data)
                    VALUES (?, ?, ?, ?)
                """, batch_data)
                conn.commit()
                print(f"    -> Đã tạo {current_count + i + batch_limit}/{TOTAL_ROWS} đơn...")
            print("[*] Khởi tạo dữ liệu hoàn tất!")
        else:
            print(f"[*] Database đã có đủ {current_count} đơn hàng. Bỏ qua seeding.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Trình tự khởi động (Khởi tạo Tọa độ -> Database -> Tạo dữ liệu)
    init_locations()
    init_db()
    seed_data()
    yield
    print("[*] Server shutdown.")

app = FastAPI(
    title="Ahamove Data Pipeline Source API",
    description="Mock Production API for Ahamove Data Lakehouse Ingestion (Bike Service Only)",
    version="4.3.0",
    lifespan=lifespan
)

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  
    return conn

# ==========================================
# 6. API ENDPOINTS
# ==========================================
@app.get("/api/v1/orders/batch")
def get_batch_orders(
    limit: int = Query(default=10000, ge=1, le=50000),
    last_id: Optional[int] = Query(default=None)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        query = "SELECT id, raw_data FROM mock_orders WHERE 1=1"
        params = []
            
        if last_id is not None:
            query += " AND id > ?"
            params.append(last_id)
            
        query += " ORDER BY id ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        
        orders = []
        for row in rows:
            item = json.loads(row["raw_data"])
            item["_db_id"] = row["id"]  
            orders.append(item)
        
        next_cursor = rows[-1]["id"] if rows else None

    return {
        "status": "success", 
        "endpoint_type": "batch", 
        "data_count": len(orders), 
        "next_cursor": next_cursor,
        "data": orders
    }

@app.get("/api/v1/orders/incremental")
def get_incremental_orders(
    since: float = Query(...),
    limit: int = Query(default=100, ge=1, le=2000)
):
    if since > time.time() + 31536000:
        raise HTTPException(status_code=400, detail="Tham số 'since' không hợp lệ.")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT raw_data FROM mock_orders 
            WHERE create_time > ? 
            ORDER BY create_time ASC 
            LIMIT ?
        """, (since, limit))
        
        rows = cursor.fetchall()
        orders = [json.loads(row["raw_data"]) for row in rows]
    
    return {
        "status": "success", 
        "endpoint_type": "incremental",
        "next_cursor": orders[-1]["create_time"] if orders else since,
        "data": orders
    }