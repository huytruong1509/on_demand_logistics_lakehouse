{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_sk',
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['gold', 'fact', 'orders']
) }}

WITH silver_orders AS (
    SELECT 
        -- Natural Keys & Core Identifiers
        order_id,
        db_id,
        source_system,
        
        -- Core Attributes
        status, sub_status, service_id, city_id, partner, assigned_by,
        user_id, supplier_id, payment_method, currency, promo_code,
        cancel_by_user, cancel_comment, is_reminded,
        
        -- Spatial & Timestamps
        from_lat, from_lng, accept_lat, accept_lng,
        create_time, order_time, accept_time, board_time, pickup_time, complete_time, cancel_time, return_time,
        
        -- Financial Metrics
        distance_km, distance_fee, stop_fee, request_fee, discount_amount, total_fee, 
        distance_price, special_request_price, stoppoint_price, voucher_discount, 
        subtotal_price, total_price, total_pay, user_main_account, user_bonus_account, 
        supplier_main_account, supplier_bonus_account,
        
        -- Feedback & Rating
        rating_by_user, comment_by_user, rating_by_supplier, comment_by_supplier,
        
        -- Audit & Lineage
        _ingest_time, _run_id, _silver_updated_at

    FROM {{ source('silver', 'orders') }}
    {% if is_incremental() %}
        -- Tối ưu: Chỉ quét các records được cập nhật từ Silver layer trong khung thời gian lookback
        WHERE _silver_updated_at >= (
            SELECT MAX(_silver_updated_at) - INTERVAL '{{ var("lookback_days", 1) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    -- ==========================================
    -- 1. SURROGATE KEYS (DIMENSION LINKS)
    -- ==========================================
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'order_id']) }} AS order_sk,
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'user_id']) }} AS user_sk,
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'supplier_id']) }} AS supplier_sk,
    {{ dbt_utils.generate_surrogate_key(['partner']) }} AS partner_sk, 
    {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'service_id']) }} AS service_sk,
    
    -- Generate Date SK dạng Integer YYYYMMDD để join siêu tốc với dim_date
    CAST(EXTRACT(YEAR FROM create_time) * 10000 + EXTRACT(MONTH FROM create_time) * 100 + EXTRACT(DAY FROM create_time) AS INTEGER) AS create_date_sk,
    
    -- ==========================================
    -- 2. NATURAL KEYS & ATTRIBUTES
    -- ==========================================
    order_id,
    db_id,
    source_system,
    status,
    sub_status,
    service_id,
    city_id,
    assigned_by,
    payment_method,
    currency,
    promo_code,
    cancel_by_user,
    cancel_comment,
    is_reminded,
    
    -- ==========================================
    -- 3. SPATIAL & TIMESTAMPS
    -- (Bắt buộc phải có create_time ở đây để Trino partition được)
    -- ==========================================
    from_lat,
    from_lng,
    accept_lat,
    accept_lng,
    create_time,
    order_time,
    accept_time,
    board_time,
    pickup_time,
    complete_time,
    cancel_time,
    return_time,
    
    -- ==========================================
    -- 4. FINANCIAL METRICS (RAW SILVER)
    -- ==========================================
    distance_km,
    distance_fee,
    stop_fee,
    request_fee,
    discount_amount,
    total_fee,
    distance_price,
    special_request_price,
    stoppoint_price,
    voucher_discount,
    subtotal_price,
    total_price,
    total_pay,
    user_main_account,
    user_bonus_account,
    supplier_main_account,
    supplier_bonus_account,
    
    -- ==========================================
    -- 5. RATINGS & FEEDBACK
    -- ==========================================
    rating_by_user,
    comment_by_user,
    rating_by_supplier,
    comment_by_supplier,

    -- ==========================================
    -- 6. DERIVED METRICS: SLA / OPERATIONS
    -- ==========================================
    date_diff('second', create_time, order_time) AS lead_time_seconds,
    date_diff('second', order_time, accept_time) AS time_to_accept_seconds,
    date_diff('minute', accept_time, pickup_time) AS time_to_pickup_minutes,
    date_diff('minute', pickup_time, complete_time) AS time_to_complete_minutes,
    
    CASE 
        WHEN cancel_time IS NOT NULL AND (pickup_time IS NULL OR cancel_time < pickup_time) THEN TRUE 
        ELSE FALSE 
    END AS is_cancelled_before_pickup,
    
    -- ==========================================
    -- 7. DERIVED METRICS: UNIT ECONOMICS
    -- Dùng COALESCE để tránh Null Propagation gây sai lệch số liệu tài chính
    -- ==========================================
    (COALESCE(total_price, 0) + COALESCE(discount_amount, 0)) AS gross_revenue,
    (COALESCE(total_pay, 0) - COALESCE(supplier_main_account, 0)) AS net_revenue,
    
    -- ==========================================
    -- 8. AUDIT & DATA LINEAGE
    -- ==========================================
    _ingest_time AS _silver_ingest_time,
    _run_id AS _silver_run_id,
    _silver_updated_at,
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS _gold_updated_at

FROM silver_orders