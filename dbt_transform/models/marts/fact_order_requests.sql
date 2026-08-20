{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_request_sk',
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['gold', 'fact', 'requests']
) }}

WITH silver_requests AS (
    SELECT 
        -- Natural Keys 
        source_system,
        db_id,
        order_id,
        
        -- Core Attributes & Metrics
        request_id,
        request_price,
        request_value,
        request_num,
        
        -- Timestamps & Lineage
        create_time,
        _ingest_time,
        _silver_updated_at

    FROM {{ source('silver', 'order_requests') }}
    {% if is_incremental() %}
        -- Tối ưu quét Delta Data
        WHERE _silver_updated_at >= (
            SELECT MAX(_silver_updated_at) - INTERVAL '{{ var("lookback_days", 1) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    -- ==========================================
    -- 1. SURROGATE KEYS & DIMENSION LINKS
    -- ==========================================
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'order_id', 'request_id']) }} AS order_request_sk,
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'order_id']) }} AS order_sk,
    
    -- Integer Key (YYYYMMDD) để JOIN siêu tốc với dim_date
    CAST(EXTRACT(YEAR FROM create_time) * 10000 + EXTRACT(MONTH FROM create_time) * 100 + EXTRACT(DAY FROM create_time) AS INTEGER) AS create_date_sk,
    
    -- ==========================================
    -- 2. NATURAL KEYS & IDENTIFIERS
    -- ==========================================
    source_system,
    db_id,
    order_id,
    request_id,
    
    -- ==========================================
    -- 3. CORE METRICS (Safe Defaults)
    -- ==========================================
    COALESCE(request_price, 0.0) AS request_price,
    COALESCE(request_value, 0.0) AS request_value,
    COALESCE(request_num, 0) AS request_num,
    
    -- ==========================================
    -- 4. TIMESTAMPS 
    -- (Bắt buộc phải có create_time để Trino partitioning)
    -- ==========================================
    create_time,
    
    -- ==========================================
    -- 5. AUDIT & DATA LINEAGE
    -- ==========================================
    _ingest_time AS _silver_ingest_time,
    _silver_updated_at,
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS _gold_updated_at

FROM silver_requests