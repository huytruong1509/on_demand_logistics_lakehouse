{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['source_system', 'db_id', 'order_id', 'request_id'],
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['silver', 'requests']
) }}

WITH base_orders AS (
    SELECT 
        CAST(_id AS VARCHAR) AS order_id,
        CAST(_db_id AS BIGINT) AS db_id,
        CAST(_source_system AS VARCHAR) AS source_system,
        -- Đảm bảo an toàn khi parse timestamp
        TRY(from_unixtime(CAST(create_time AS DOUBLE))) AT TIME ZONE 'UTC' AS create_time,
        _ingest_time,
        CAST(requests AS VARCHAR) AS raw_requests_json,
        -- Deduplicate lấy record mới nhất dựa trên _ingest_time
        ROW_NUMBER() OVER(
            PARTITION BY _source_system, _db_id, _id 
            ORDER BY _ingest_time DESC
        ) AS rn
    FROM {{ source('bronze_logistics', 'raw_orders') }}
    WHERE requests IS NOT NULL 
      AND TRIM(requests) NOT IN ('', '[]')
    
    {% if is_incremental() %}
        -- Lọc data incremental (Lưu ý: Nếu bảng source quá lớn, cân nhắc dùng 
        -- dbt run_query macro để lấy max_ingest_time thành giá trị tĩnh đẩy xuống)
        AND _ingest_time >= (SELECT MAX(_ingest_time) - INTERVAL '{{ var("lookback_days", 1) }}' DAY FROM {{ this }})
    {% endif %}
),

parsed_json AS (
    SELECT 
        order_id,
        db_id,
        source_system,
        create_time,
        _ingest_time,
        -- Ép thẳng về ARRAY(ROW) để Strong Typing, an toàn và hiệu năng cao
        TRY(CAST(json_parse(raw_requests_json) AS ARRAY(ROW(
            _id VARCHAR,
            price DOUBLE,
            value DOUBLE,
            num INTEGER
        )))) AS requests_array
    FROM base_orders
    WHERE rn = 1 -- Lọc deduplicate sớm nhất có thể để giảm tải cho JSON parse
)

SELECT 
    bo.source_system,
    bo.db_id,
    bo.order_id,
    t._id AS request_id, -- [FIX 1]: Thêm dấu phẩy còn thiếu
    
    -- [FIX 2]: Sửa t.req.price thành t.price (vì UNNEST alias t đã định nghĩa sẵn các cột phẳng)
    COALESCE(TRY_CAST(t.price AS DECIMAL(18,2)), 0.00) AS request_price,
    COALESCE(TRY_CAST(t.value AS DECIMAL(18,2)), 0.00) AS request_value,
    COALESCE(TRY_CAST(t.num AS INTEGER), 0) AS request_num,
    
    bo.create_time,
    bo._ingest_time,
    current_timestamp AT TIME ZONE 'UTC' AS _silver_updated_at

FROM parsed_json bo
CROSS JOIN UNNEST(bo.requests_array) AS t(_id, price, value, num)
WHERE bo.requests_array IS NOT NULL 
  AND t._id IS NOT NULL
ORDER BY bo.create_time