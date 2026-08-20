-- Tạo User và Database độc lập cho Airflow
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- Cấp quyền schema (Bắt buộc với Postgres 15+)
\c airflow_db;
GRANT ALL ON SCHEMA public TO airflow_user;

-- 2. Tạo User và Database độc lập cho Superset
CREATE USER superset_user WITH PASSWORD 'superset_pass';
CREATE DATABASE superset_db;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset_user;

\c superset_db;
GRANT ALL ON SCHEMA public TO superset_user;