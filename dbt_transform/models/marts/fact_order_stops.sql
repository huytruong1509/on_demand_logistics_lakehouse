{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_stop_sk',
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['gold', 'fact', 'stops']
) }}

WITH silver_stops AS (
    SELECT 
        -- Natural Keys & Core Identifiers
        source_system,
        db_id,
        order_id,
        stop_index,
        
        -- Core Attributes
        stop_name,
        stop_mobile,
        stop_address,
        building,
        apt_number,
        stop_status,
        tracking_number,
        stop_remarks,
        redelivery_note,
        
        -- Verification & POD Attributes
        is_pod_required,
        is_verification_required,
        pod_info,
        pod_image_url,
        
        -- Ratings & Feedback
        rating_by_receiver,
        comment_by_receiver,
        complete_comment,
        fail_comment,
        
        -- Spatial & Coordinates
        stop_lat,
        stop_lng,
        complete_lat,
        complete_lng,
        fail_lat,
        fail_lng,
        
        -- Financials & Timestamps
        cod_amount,
        create_time,
        complete_time,
        fail_time,
        
        -- Lineage
        _ingest_time,
        _silver_updated_at

    FROM {{ source('silver', 'order_stops') }}
    {% if is_incremental() %}
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
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'order_id', 'CAST(stop_index AS VARCHAR)']) }} AS order_stop_sk,
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'order_id']) }} AS order_sk,
    
    -- Integer Key (YYYYMMDD) hỗ trợ Partition Pruning siêu tốc khi JOIN với dim_date
    CAST(EXTRACT(YEAR FROM create_time) * 10000 + EXTRACT(MONTH FROM create_time) * 100 + EXTRACT(DAY FROM create_time) AS INTEGER) AS create_date_sk,
    
    -- ==========================================
    -- 2. NATURAL KEYS & IDENTIFIERS
    -- ==========================================
    source_system,
    db_id,
    order_id,
    stop_index,
    
    -- ==========================================
    -- 3. CORE ATTRIBUTES
    -- ==========================================
    stop_status,
    tracking_number,
    stop_name,
    stop_mobile, -- Có thể áp dụng hàm HASH() nếu quy định cty yêu cầu bảo mật PII
    stop_address,
    building,
    apt_number,
    stop_remarks,
    redelivery_note,
    
    -- ==========================================
    -- 4. VERIFICATION & POD
    -- ==========================================
    is_pod_required,
    is_verification_required,
    pod_info,
    pod_image_url,
    
    -- ==========================================
    -- 5. RATINGS & FEEDBACK
    -- ==========================================
    rating_by_receiver,
    comment_by_receiver,
    complete_comment,
    fail_comment,
    
    -- ==========================================
    -- 6. METRICS & FINANCIALS
    -- ==========================================
    cod_amount,
    
    -- ==========================================
    -- 7. SPATIAL & TIMESTAMPS
    -- (create_time là bắt buộc để Trino phân vùng dữ liệu)
    -- ==========================================
    stop_lat,
    stop_lng,
    complete_lat,
    complete_lng,
    fail_lat,
    fail_lng,
    create_time,
    complete_time,
    fail_time,
    
    -- ==========================================
    -- 8. DERIVED METRICS & GEOSPATIAL ANALYSIS
    -- ==========================================
    CASE 
        WHEN UPPER(stop_status) = 'COMPLETED' AND complete_time IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_successful_stop,
    
    CASE 
        WHEN is_pod_required = TRUE THEN (pod_image_url IS NOT NULL)
        ELSE NULL 
    END AS is_pod_compliant,
    
    -- Bọc điều kiện kiểm tra NULL trước khi tính toán khoảng cách cầu học (meters)
    CASE 
        WHEN stop_lat IS NOT NULL AND stop_lng IS NOT NULL 
             AND complete_lat IS NOT NULL AND complete_lng IS NOT NULL THEN
            ST_Distance(
                to_spherical_geography(ST_Point(stop_lng, stop_lat)), 
                to_spherical_geography(ST_Point(complete_lng, complete_lat))
            )
        ELSE NULL 
    END AS distance_to_target_meters,
    
    -- Thời gian hoàn thành điểm dừng (phút)
    CASE 
        WHEN complete_time IS NOT NULL THEN date_diff('minute', create_time, complete_time)
        ELSE NULL 
    END AS stop_duration_minutes,
    
    -- ==========================================
    -- 9. AUDIT & DATA LINEAGE
    -- ==========================================
    _ingest_time AS _silver_ingest_time,
    _silver_updated_at,
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS _gold_updated_at

FROM silver_stops