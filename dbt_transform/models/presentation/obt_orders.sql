{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_sk',
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['gold', 'obt', 'superset', 'orders']
) }}

WITH base_orders AS (
    -- Lấy Fact cốt lõi: 1 dòng = 1 đơn hàng
    SELECT 
        order_sk,
        user_sk,
        supplier_sk,
        partner_sk,
        create_date_sk,
        order_id,
        city_id,
        service_id,
        status,
        sub_status,
        assigned_by,
        payment_method,
        currency,
        promo_code,
        cancel_by_user,
        cancel_comment,
        is_reminded,
        from_lat,
        from_lng,
        create_time,
        order_time,
        accept_time,
        board_time,
        pickup_time,
        complete_time,
        cancel_time,
        distance_km,
        distance_fee,
        total_fee,
        discount_amount,
        gross_revenue,
        net_revenue,
        total_price,
        rating_by_user,
        rating_by_supplier,
        lead_time_seconds,
        time_to_accept_seconds,
        time_to_pickup_minutes,
        time_to_complete_minutes,
        is_cancelled_before_pickup,
        user_bonus_account,
        supplier_bonus_account,
        accept_lat,
        accept_lng,
        return_time,
        stop_fee,
        request_fee,
        distance_price,
        special_request_price,
        stoppoint_price,
        voucher_discount,
        subtotal_price,
        total_pay,
        user_main_account,
        supplier_main_account,
        comment_by_user,
        comment_by_supplier
    FROM {{ ref('fact_orders') }}
    {% if is_incremental() %}
        -- Tối ưu quét Delta Data
        WHERE create_time >= (
            SELECT MAX(create_time) - INTERVAL '{{ var("lookback_days", 3) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
),

agg_order_stops AS (
    -- Pre-aggregate fact_order_stops để tránh fan-out khi join (1 order có >= 2 stops)
    SELECT 
        order_sk,
        COUNT(order_stop_sk) AS total_stops,
        COUNT_IF(is_successful_stop) AS total_successful_stops,
        COUNT_IF(is_pod_required = TRUE) AS total_pod_required,
        COUNT_IF(is_pod_compliant = TRUE) AS total_pod_compliant,
        SUM(COALESCE(cod_amount, 0.0)) AS total_cod_amount,
        SUM(COALESCE(stop_duration_minutes, 0)) AS total_stop_duration_minutes
    FROM {{ ref('fact_order_stops') }}
    {% if is_incremental() %}
        WHERE create_time >= (
            SELECT MAX(create_time) - INTERVAL '{{ var("lookback_days", 3) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
    GROUP BY 1
),

agg_order_requests AS (
    -- Pre-aggregate fact_order_requests (Các dịch vụ cộng thêm như COD)
    SELECT 
        order_sk,
        ARRAY_AGG(request_id) AS request_id_list,
        SUM(COALESCE(request_price, 0.0)) AS total_request_price,
        SUM(COALESCE(request_value, 0.0)) AS total_request_value,
        SUM(COALESCE(request_num, 0)) AS total_request_num
    FROM {{ ref('fact_order_requests') }}
    {% if is_incremental() %}
        WHERE create_time >= (
            SELECT MAX(create_time) - INTERVAL '{{ var("lookback_days", 3) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
    GROUP BY 1
)

SELECT 
    -- ==========================================
    -- 1. IDENTIFIERS (Mã định danh)
    -- ==========================================
    o.order_sk,
    o.order_id,
    
    -- ==========================================
    -- 2. DIMENSIONS CƠ BẢN (Từ Fact)
    -- ==========================================
    o.status AS order_status,
    o.sub_status,
    o.service_id,
    o.city_id,
    o.payment_method,
    o.promo_code,
    o.cancel_by_user,
    o.cancel_comment,

    o.assigned_by,
    o.currency,
    o.is_reminded,
    o.comment_by_user,
    o.comment_by_supplier,
    
    -- ==========================================
    -- 3. DENORMALIZED DIMENSIONS (Trải phẳng Dimensions)
    -- ==========================================
    -- Dim Date
    dd.full_date AS create_date,
    dd.year_month,
    dd.day_name,
    dd.is_weekend,
    EXTRACT(HOUR FROM o.create_time) AS create_hour, -- Phục vụ heatmap theo giờ
    
    -- Dim User (Sender/Customer)
    COALESCE(du.user_id, 'UNKNOWN') AS user_id,
    COALESCE(du.user_name, 'UNKNOWN') AS user_name,
    
    -- Dim Supplier (Driver/Rider)
    COALESCE(ds.supplier_id, 'UNKNOWN') AS supplier_id,
    COALESCE(ds.supplier_name, 'UNKNOWN') AS supplier_name,
    
    -- Dim Partner (B2B/Merchant Partner)
    COALESCE(dp.partner_name, 'RETAIL') AS partner_name,
    
    -- ==========================================
    -- 4. GEOSPATIAL & OPERATIONS
    -- ==========================================
    o.from_lat,
    o.from_lng,
    o.distance_km,
    COALESCE(s.total_stops, 0) AS total_stops,
    COALESCE(s.total_successful_stops, 0) AS total_successful_stops,

    o.accept_lat,
    o.accept_lng,
    
    -- ==========================================
    -- 5. FINANCIAL METRICS (Doanh thu & Chi phí)
    -- ==========================================
    o.gross_revenue,
    o.net_revenue,
    o.total_fee,
    o.discount_amount,
    o.user_bonus_account,
    o.supplier_bonus_account,
    COALESCE(s.total_cod_amount, 0.0) AS total_cod_amount,
    COALESCE(r.total_request_price, 0.0) AS total_request_price,
    COALESCE(r.total_request_value, 0.0) AS total_request_value,

    o.distance_fee,
    o.total_price,
    o.stop_fee,
    o.request_fee,
    o.distance_price,
    o.special_request_price,
    o.stoppoint_price,
    o.voucher_discount,
    o.subtotal_price,
    o.total_pay,
    o.user_main_account,
    o.supplier_main_account,
    
    -- ==========================================
    -- 6. SLA & TIMESTAMPS
    -- ==========================================
    o.create_time,
    o.order_time,
    o.accept_time,
    o.board_time,
    o.pickup_time,
    o.complete_time,
    o.cancel_time,
    o.return_time,
    
    o.lead_time_seconds,
    o.time_to_accept_seconds,
    o.time_to_pickup_minutes,
    o.time_to_complete_minutes,
    o.is_cancelled_before_pickup,
    
    -- ==========================================
    -- 7. QUALITY & COMPLIANCE
    -- ==========================================
    o.rating_by_user,
    o.rating_by_supplier,
    COALESCE(s.total_pod_required, 0) AS total_pod_required,
    COALESCE(s.total_pod_compliant, 0) AS total_pod_compliant,
    CASE 
        WHEN s.total_pod_required > 0 AND s.total_pod_required = s.total_pod_compliant THEN TRUE
        ELSE FALSE
    END AS is_fully_pod_compliant,
    
    -- ARRAY format hỗ trợ Superset filter IN
    r.request_id_list,

    -- ==========================================
    -- 8. AUDIT COLUMNS
    -- ==========================================
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS _obt_updated_at

FROM base_orders o

-- Join với Dim Date siêu tốc qua Integer SK (YYYYMMDD)
LEFT JOIN {{ ref('dim_date') }} dd 
    ON o.create_date_sk = dd.date_sk

-- Join Dimensions
LEFT JOIN {{ ref('dim_user') }} du 
    ON o.user_sk = du.user_sk
LEFT JOIN {{ ref('dim_supplier') }} ds 
    ON o.supplier_sk = ds.supplier_sk
LEFT JOIN {{ ref('dim_partner') }} dp 
    ON o.partner_sk = dp.partner_sk

-- Join Aggregated Facts
LEFT JOIN agg_order_stops s 
    ON o.order_sk = s.order_sk
LEFT JOIN agg_order_requests r 
    ON o.order_sk = r.order_sk