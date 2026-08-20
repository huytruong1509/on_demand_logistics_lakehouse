🚚 On-Demand Logistics Lakehouse: Pre-Pickup SLA Analytics System

[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-blue.svg)](#-kiến-trúc-hệ-thống--tech-stack)
[![Catalog](https://img.shields.io/badge/Catalog-Project%20Nessie-orange.svg)](https://projectnessie.org/)
[![Storage](https://img.shields.io/badge/Format-Apache%20Iceberg-cyan.svg)](https://iceberg.apache.org/)
[![Query Engine](https://img.shields.io/badge/Query%20Engine-Trino%20476-red.svg)](https://trino.io/)
[![Orchestrator](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-teal.svg)](https://airflow.apache.org/)
[![Transform](https://img.shields.io/badge/Transform-dbt%20%2B%20PySpark-green.svg)](https://www.getdbt.com/)
[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-GitHub%20Pages-brightgreen.svg)](https://huytruong1509.github.io/ahamove_pre_pickup_sla_dashboard/)

---

## 📌 1. Tổng Quan Dự Án (Project Overview)

**On-Demand Logistics Lakehouse** là hệ thống dữ liệu được xây dựng theo mô hình Data Lakehouse nhằm xử lý và theo dõi chỉ số **SLA Pre-pickup** (giai đoạn từ khi tạo đơn đến khi tài xế lấy hàng thành công) trong mô hình giao hàng nhanh theo yêu cầu (On-Demand Logistics).

Hệ thống hỗ trợ giải quyết các bài toán vận hành cốt lõi:

- **Tối ưu hóa thời gian chờ (Latency Tracking):** Đo lường chính xác khoảng thời gian từ lúc tài xế chấp nhận đơn đến khi tới điểm lấy hàng (_Phase Accept ➔ Boarding_).
- **Phát hiện điểm nghẽn SLA (Bottleneck Identification):** Bóc tách nguyên nhân khiến tài xế hủy đơn, lấy hàng chậm hoặc các khu vực thường xuyên bị vỡ SLA.
- **Theo dõi hiệu suất (Performance Tracking):** Thống kê tỷ lệ hoàn thành SLA theo từng quận huyện, khung giờ và loại dịch vụ (_Siêu Tốc, Nhanh, 4H, Đồng Giá_).

> 💡 **Data Source:** Dữ liệu đầu vào được mô phỏng (Mock Data Generator) dựa trên [Ahamove Order API Data Model](https://developers.ahamove.com/docs/api-reference/order-apis/data-model)

> 👉 **Live Interactive Dashboard:** [Ahamove Pre-Pickup SLA Dashboard](https://huytruong1509.github.io/ahamove_pre_pickup_sla_dashboard/)

---

## 🏗️ 2. Kiến Trúc Hệ Thống (Architecture & Tech Stack)

Hệ thống tuân thủ mô hình **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** được điều phối tự động hoàn toàn qua **Apache Airflow**:

```text
                     +-----------------------+
                     |    Data Source API    |
                     +-----------+-----------+
                                 |
                                 | (Python API Extractor - Ingestion)
                                 v
+-----------------------------------------------------------------+
| BRONZE LAYER                                                    |
| Raw Parquet / JSON -> MinIO S3 Object Storage                   |
+--------------------------------+--------------------------------+
                                 |
                                 | (PySpark Processing)
                                 v
+-----------------------------------------------------------------+
| SILVER LAYER                                                    |
| Apache Iceberg Tables -> Nessie Catalog + MinIO S3              |
+--------------------------------+--------------------------------+
                                 |
                                 | (dbt-Trino Modeling)
                                 v
+-----------------------------------------------------------------+
| GOLD LAYER                                                      |
| Star Schema Data Marts -> Trino Query Engine                    |
+--------------------------------+--------------------------------+
                                 |
      +--------------------------+--------------------------+
      |                          |                          |
      v                          v                          v
+---------------+      +-------------------+      +--------------------+
|   Superset    |      |    Jupyter Lab    |      |    GitHub Pages    |
| Live Dashboard|      |  Data Exploration |      |    Documentation   |
+---------------+      +-------------------+      +--------------------+
```

### 🛠 Công Nghệ Sử Dụng (Tech Stack)

| Hợp phần                 | Công nghệ                       | Vai trò & Trách nhiệm trong Pipeline                                                    |
| :----------------------- | :------------------------------ | :-------------------------------------------------------------------------------------- |
| **Data Source**          | FastAPI / Python Mock Service   | Mô phỏng hệ thống core logistics phát sinh đơn hàng                                     |
| **Ingestion (Bronze)**   | Python                          | Extract dữ liệu API định kỳ, lưu trữ raw payload dưới dạng Parquet/JSON vào MinIO       |
| **Processing (Silver)**  | PySpark (Spark Serverless)      | Làm sạch, chuẩn hóa kiểu dữ liệu, khử trùng lặp (Deduplication), ghi vào Apache Iceberg |
| **Transform (Gold)**     | dbt (`dbt-trino`)               | Xây dựng Data Mart, Star Schema (Dim/Fact tables), tính toán các KPIs & SLA Metrics     |
| **Query Engine**         | Trino 476                       | Distributed SQL Engine truy vấn tốc độ cao trực tiếp trên Iceberg/Nessie Lakehouse      |
| **Catalog & Versioning** | Project Nessie                  | Quản lý Metadata & Data Version Control (Git-like: branch, commit, merge cho Iceberg)   |
| **Storage Layer**        | MinIO (S3-compatible)           | Khối lưu trữ Object Storage cho dữ liệu Lakehouse (Bronze, Silver, Gold buckets)        |
| **Orchestration**        | Apache Airflow (Local Executor) | Lập lịch và điều phối toàn bộ workflow từ Extract API ➔ Spark ➔ dbt ➔ Refresh Data      |
| **Metadata DB**          | PostgreSQL 15                   | Lưu trữ Metadata hệ thống cho Airflow, Superset, Nessie Catalog                         |
| **Analytics & BI**       | Apache Superset & GitHub Pages  | Trực quan hóa dữ liệu SLA, theo dõi chỉ số hoạt động tài xế và đơn hàng                 |
| **Data Exploration**     | Jupyter Lab                     | Môi trường cho Data Analyst/Scientist thực hiện ad-hoc query & phân tích nâng cao       |

---

## 📂 3. Cấu Trúc Thư Mục Dự Án (Project Structure)

```text
logistics-lakehouse/
├── dags/                                 # Airflow DAGs & Pipeline Code
│   ├── schemas/                          # Schemas kiểm tra định dạng dữ liệu (Data Validation)
│   ├── utils/                            # Helper modules (MinIO connection, Logging, Trino client)
│   ├── config.py                         # Cấu hình biến môi trường pipeline
│   ├── dbt_gold.py                       # DAG chạy dbt build cho Gold layer
│   ├── dbt_presentation.py               # DAG tổng hợp presentation layer & docs
│   ├── dbt_silver_gold.py                # DAG liên kết chuyển đổi từ Silver sang Gold
│   ├── logistics_full_load.py            # DAG chạy Backfill / Full Load dữ liệu lịch sử
│   ├── logistics_incremental_load.py     # DAG Incremental Load thời gian thực
│   ├── logistics_setup_infrastructure.py # DAG khởi tạo Bucket, Iceberg Namespace, Nessie Branch
│   └── silver_spark_serverless.py        # Airflow Operator gọi PySpark Job cho Silver layer
├── dashboards/                           # File export & cấu hình Superset Dashboard
├── data_source/                          # Mock API Data Source service (FastAPI Engine)
├── dbt_transform/                        # Project dbt (Models, Macros, Tests, Docs)
│   ├── models/
│   │   └── marts/                        # Fact & Dim tables (Gold Layer)
│   ├── dbt_project.yml                   # Cấu hình dbt project
│   └── packages.yml                      # Khai báo dbt packages (dbt_utils)
├── infrastructure/                       # Container orchestration & Configs
│   ├── postgres_init/                    # Scripts khởi tạo DB Airflow/Superset/Nessie
│   ├── trino_config/                     # Config Coordinator, Worker & Catalog Trino
│   ├── .env                              # Biến môi trường hệ thống
│   ├── docker-compose.yml                # Container orchestration chính
│   ├── Dockerfile.airflow                # Custom image Airflow tích hợp dbt & Spark
│   ├── Dockerfile.superset               # Custom image Superset với Trino driver
│   └── superset_config.py                # Cấu hình Superset
├── notebooks/                            # Jupyter Notebooks cho Data Analytics & Exploration
└── spark_jobs/                           # Các PySpark scripts xử lý Silver Layer
```

## 🔄 4. Chi Tiết Luồng Xử Lý Dữ Liệu (Medallion Architecture)

### 🟤 1. Bronze Layer (Raw Ingestion)

- **Công cụ:** Python (`requests` + Airflow `PythonOperator`).
- **Quy trình:**
  - Gọi REST API từ `data_source` thu thập dữ liệu thô: `orders`
  - Giữ nguyên định dạng gốc (Raw JSON Payload), đính kèm timestamp nhận dữ liệu (`ingestion_at`, `source_system`,..).
  - Lưu trữ dưới dạng file `.parquet` phân vùng theo thời gian `year=YYYY/month=MM/day=DD` tại MinIO bucket `s3://lakehouse/bronze/`.

### 🥈 2. Silver Layer (Standardized & Cleansed Iceberg Tables)

- **Công cụ:** PySpark (`spark-submit` / DockerOperator).
- **Quy trình:**
  - Đọc dữ liệu mới từ Bronze bucket.
  - Chuẩn hóa Schema
  - Khử trùng lặp đơn hàng theo `order_id` dựa trên mốc cập nhật muộn nhất (`updated_at`).
  - Ghi dữ liệu vào **Apache Iceberg Table** được quản lý bởi **Nessie Catalog** (`s3://lakehouse/silver/`).
  - Hỗ trợ tính năng Time-Travel và Schema Evolution linh hoạt.

### 🥇 3. Gold Layer (Data Marts & SLA Modeling)

- **Công cụ:** dbt kết hợp Trino Query Engine.
- **Mô hình hóa:** Thiết kế Star Schema chuẩn Ralph Kimball tối ưu cho BI & Data Analytics. Hệ thống áp dụng Surrogate Key (`_sk`) và Date SK dạng Integer (`YYYYMMDD`) để tối ưu hóa truy vấn JOIN và Partition Pruning.

#### 📐 Bảng Chiều (Dimension Tables)

- **`dim_date`**: Bảng chiều thời gian tự động sinh (Date Spine 2020–2030), tích hợp sẵn các chỉ số BI như `is_weekend`, `is_first_day_of_month`, `is_last_day_of_month`.
- **`dim_supplier`**: Bảng chiều tài xế / nhà cung cấp (`supplier_id`, `supplier_name`), xử lý khử trùng lặp dữ liệu theo chuẩn SCD Type 1.
- **`dim_user`**: Bảng chiều người dùng / khách hàng đặt đơn (`user_id`, `user_name`), áp dụng chuẩn hóa và khử trùng lặp SCD Type 1.
- **`dim_partner`**: Bảng chiều đối tác vận chuyển / tích hợp hệ thống (`partner_name`).

#### 📊 Bảng Sự Kiện (Fact Tables)

- **`fct_orders`**: Bảng sự kiện đơn hàng cốt lõi (phân vùng theo `day(create_time)`). Lưu trữ đầy đủ vòng đời mốc thời gian (_create_, _accept_, _pickup_, _complete_, _cancel_), đo lường các chỉ số SLA (`time_to_accept_seconds`, `time_to_pickup_minutes`, `is_cancelled_before_pickup`) cùng các chỉ số tài chính (Gross/Net Revenue).
- **`fct_order_stops`**: Bảng sự kiện các điểm dừng giao nhận hàng (phân vùng theo `day(create_time)`), hỗ trợ tính khoảng cách thực tế di chuyển (`distance_to_target_meters`), thời gian dừng (`stop_duration_minutes`), xác thực POD và tiền thu hộ COD.
- **`fct_order_requests`**: Bảng sự kiện chi tiết các yêu cầu / dịch vụ đặc biệt đi kèm theo từng đơn hàng.

## 📊 5. Executive Dashboard

Dashboard hỗ trợ Ops Team tracking vận hành và tìm ra nguyên nhân gây trễ SLA:

- **1. LTA Dashboard (SLA Health):** Theo dõi tổng quan tỷ lệ đạt SLA. So sánh thời gian xử lý trung bình (P50) và nhóm trễ nhất (P90) để phát hiện sự cố kịp thời.
- **2. Cancel Rate Breakdown:** Phân tích tỷ lệ hủy đơn trước khi tài xế lấy hàng (_Pre-Boarding_) theo 3 nhóm nguyên nhân: Khách hủy (_User Cancel_), Tài xế từ chối (_Driver Cancel_) và Hệ thống hết giờ (_System Timeout_).
- **3. Hotspot Matrix:** Bản đồ nhiệt (Heatmap) thể hiện tỷ lệ trễ SLA theo từng `[Quận x Loại dịch vụ]` trong các tình huống thời tiết xấu (như mưa lớn, ngập lụt) để xác định điểm nóng vỡ SLA.
- **4. Dispatch Dynamics:** Theo dõi hiệu quả của logic điều phối giữa hai hình thức: Gom đơn (_Batching_) và Giao ngay (_On-Demand_).
- **5. Spatial Anomaly Scatter:** Biểu đồ phân tán (_Scatter Plot_) giúp phát hiện các đơn hàng distance ngắn nhưng có thời gian lấy hàng lâu bất thường.
- **6. B2B Merchant Friction:** Đo lường thời gian tài xế chờ lấy hàng tại các Kho B2B / Hub tập kết để phát hiện các điểm nghẽn làm chậm quá trình vận chuyển.

## 🚀 6. (Getting Started)

### Yêu Cầu (Prerequisites)

- **Docker Engine** >= 20.10.0 & **Docker Compose** v2+
- Cấu hình đề xuất: tối thiểu **8 CPU Cores** & **12 GB RAM** khả dụng (để chạy mượt Trino Cluster & Spark).

### Các Bước Triển Khai

#### Bước 1: Clone Repository & Khởi tạo môi trường

```bash
git clone [https://github.com/huytruong1509/on_demand_logistics_lakehouse.git](https://github.com/huytruong1509/on_demand_logistics_lakehouse.git)
cd logistics-lakehouse/infrastructure

# Kiểm tra file biến môi trường .env
cp .env.example .env
```

#### Bước 2: Khởi chạy cụm Docker Containers

```bash
docker compose up -d
```

#### Bước 3: Kiểm tra trạng thái các Services

```bash
docker compose ps
```

### 🌐 Cổng Dịch Vụ Hệ Thống (System Endpoints)

| Service               | Port    | URL / Interface                                          | Tài khoản mặc định    |
| :-------------------- | :------ | :------------------------------------------------------- | :-------------------- |
| **Airflow Webserver** | `8081`  | [http://localhost:8081](http://localhost:8081)           | `admin` / `admin`     |
| **Trino Coordinator** | `8080`  | [http://localhost:8080](http://localhost:8080)           | `admin` (No password) |
| **MinIO Console**     | `9001`  | [http://localhost:9001](http://localhost:9001)           | `admin` / `password`  |
| **Superset BI**       | `8088`  | [http://localhost:8088](http://localhost:8088)           | `admin` / `admin`     |
| **dbt Documentation** | `8082`  | [http://localhost:8082](http://localhost:8082)           | Public                |
| **Nessie Catalog**    | `19120` | [http://localhost:19120](http://localhost:19120)         | API Endpoint          |
| **Jupyter Lab**       | `8888`  | [http://localhost:8888](http://localhost:8888)           | Token: `admin`        |
| **Data Source API**   | `8005`  | [http://localhost:8005/docs](http://localhost:8005/docs) | Swagger Docs          |

---

## ⚡ 7. Vận Hành Pipelines Cùng Apache Airflow

Sau khi hệ thống khởi động thành công, truy cập **Airflow UI** (`http://localhost:8081`) và kích hoạt các DAGs theo thứ tự:

1. **`logistics_setup_infrastructure`**: Khởi tạo Minio bucket, và thiết lập cấu hình bảng Iceberg ban đầu.
2. **`logistics_full_load`**: Nạp dữ liệu lịch sử (Historical Backfill) từ API vào Bronze ➔ Spark Silver ➔ dbt Gold.
3. **`logistics_incremental_load`**: Lập lịch chạy định kỳ để cập nhật dữ liệu mới.
4. **`dbt_presentation`**: Re-generate dbt docs và đẩy báo cáo vào Nginx viewer (`http://localhost:8082`).

---
