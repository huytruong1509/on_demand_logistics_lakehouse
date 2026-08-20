{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'date_sk',
    properties = {
        'format': "'PARQUET'"
    },
    tags = ['gold', 'dimension', 'date']
) }}

WITH date_spine AS (
    -- Tự động sinh danh sách ngày từ năm 2020 đến 2030 (Thay đổi range theo nhu cầu thực tế của cty)
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2020-01-01' AS DATE)",
        end_date="CAST('2030-12-31' AS DATE)"
    ) }}
),

date_data AS (
    SELECT 
        CAST(date_day AS DATE) AS full_date,
        
        -- Generate Integer SK dạng YYYYMMDD (chuẩn Ralph Kimball, hỗ trợ partition pruning siêu tốc)
        CAST(EXTRACT(YEAR FROM date_day) * 10000 + EXTRACT(MONTH FROM date_day) * 100 + EXTRACT(DAY FROM date_day) AS INTEGER) AS date_sk,
        
        -- Các thành phần cơ bản (Numeric)
        EXTRACT(YEAR FROM date_day) AS year_num,
        EXTRACT(QUARTER FROM date_day) AS quarter_num,
        EXTRACT(MONTH FROM date_day) AS month_num,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DAY_OF_WEEK FROM date_day) AS day_of_week,
        
        -- Các thành phần mở rộng phục vụ BI
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        
        -- Các trường chuỗi (String) phục vụ trực quan hoá trên Dashboard
        date_format(date_day, '%Y-%m') AS year_month,
        date_format(date_day, '%M') AS month_name,
        date_format(date_day, '%W') AS day_name,
        
        -- Logic đánh dấu cuối tuần (Trino: 1 = Thứ 2, 7 = Chủ nhật)
        CASE 
            WHEN EXTRACT(DAY_OF_WEEK FROM date_day) IN (6, 7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Đánh dấu ngày đầu/cuối tháng
        CASE 
            WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_first_day_of_month,
        
        CASE 
            WHEN CAST(date_day AS DATE) = LAST_DAY_OF_MONTH(CAST(date_day AS DATE)) THEN TRUE 
            ELSE FALSE 
        END AS is_last_day_of_month
        
    FROM date_spine
)

SELECT 
    d.date_sk,
    d.full_date,
    d.year_num,
    d.quarter_num,
    d.month_num,
    d.month_name,
    d.year_month,
    d.day_of_month,
    d.day_of_year,
    d.day_of_week,
    d.day_name,
    d.week_of_year,
    d.is_weekend,
    d.is_first_day_of_month,
    d.is_last_day_of_month,
    
    {% if is_incremental() %}
        COALESCE(t.created_at, CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE)) AS created_at,
    {% else %}
        CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS created_at,
    {% endif %}
    
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS updated_at

FROM date_data d
{% if is_incremental() %}
LEFT JOIN {{ this }} t 
    ON t.date_sk = d.date_sk
{% endif %}