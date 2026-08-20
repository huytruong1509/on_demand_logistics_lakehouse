{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['source_system', 'db_id', 'order_id'],
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['silver', 'orders']
) }}

WITH raw_source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY _source_system, _db_id, _id 
            ORDER BY _ingest_time DESC
        ) AS rn
    FROM {{ source('bronze_logistics', 'raw_orders') }}
    -- FIX LỖI: Bổ sung WHERE 1=1 để tránh lỗi syntax khi compile Jinja
    WHERE 1=1 
    {% if is_incremental() %}
        AND _ingest_time >= (SELECT MAX(_ingest_time) - INTERVAL '{{ var("lookback_days", 3) }}' DAY FROM {{ this }})
    {% endif %}
)

SELECT 
    -- Key Identifiers
    CAST(_id AS VARCHAR) AS order_id,
    CAST(_db_id AS BIGINT) AS db_id,
    CAST(_source_system AS VARCHAR) AS source_system,
    
    -- Order Status & Service Identifiers
    COALESCE(UPPER(TRIM(CAST(status AS VARCHAR))), 'UNKNOWN') AS status,
    COALESCE(UPPER(TRIM(CAST(sub_status AS VARCHAR))), 'UNKNOWN') AS sub_status,
    CAST(service_id AS VARCHAR) AS service_id,
    COALESCE(UPPER(TRIM(CAST(city_id AS VARCHAR))), 'UNKNOWN') AS city_id,
    
    -- Entities & Actors
    CAST(user_id AS VARCHAR) AS user_id,
    CAST(user_name AS VARCHAR) AS user_name,
    CAST(supplier_id AS VARCHAR) AS supplier_id,
    CAST(supplier_name AS VARCHAR) AS supplier_name,
    COALESCE(CAST(partner AS VARCHAR), 'NON_PARTNER') AS partner,
    CAST(assigned_by AS VARCHAR) AS assigned_by,
    
    -- Timestamps (Tối ưu cast rõ ràng để dùng làm partition key)
    CAST(TRY(from_unixtime(CAST(create_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS create_time,
    CAST(TRY(from_unixtime(CAST(order_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS order_time,
    CAST(TRY(from_unixtime(CAST(idle_until AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS idle_until,
    CAST(TRY(from_unixtime(CAST(accept_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS accept_time,
    CAST(TRY(from_unixtime(CAST(board_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS board_time,
    CAST(TRY(from_unixtime(CAST(pickup_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS pickup_time,
    CAST(TRY(from_unixtime(CAST(complete_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS complete_time,
    CAST(TRY(from_unixtime(CAST(cancel_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS cancel_time,
    CAST(TRY(from_unixtime(CAST(return_time AS DOUBLE))) AT TIME ZONE 'UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS return_time,
    
    -- Spatial & Acceptance Metrics
    TRY_CAST(accept_lat AS DOUBLE) AS accept_lat,
    TRY_CAST(accept_lng AS DOUBLE) AS accept_lng,
    COALESCE(TRY_CAST(accept_distance AS DOUBLE), 0.0) AS accept_distance,
    COALESCE(TRY_CAST(accept_duration AS DOUBLE), 0.0) AS accept_duration,
    
    -- Cancellation Notes
    COALESCE(CAST(cancel_comment AS VARCHAR), 'NOT_CANCELLED') AS cancel_comment,
    COALESCE(CAST(cancel_by_user AS VARCHAR), 'NOT_CANCELLED') AS cancel_by_user,
    
    -- Financials & Pricing Breakdown
    COALESCE(UPPER(TRIM(CAST(currency AS VARCHAR))), 'VND') AS currency,
    COALESCE(CAST(promo_code AS VARCHAR), 'NO_PROMO') AS promo_code,
    COALESCE(UPPER(TRIM(CAST(payment_method AS VARCHAR))), 'UNKNOWN') AS payment_method,
    COALESCE(TRY_CAST(distance AS DOUBLE), 0.0) AS distance_km,
    COALESCE(TRY_CAST(distance_fee AS DECIMAL(18, 2)), 0.00) AS distance_fee,
    COALESCE(TRY_CAST(stop_fee AS DECIMAL(18, 2)), 0.00) AS stop_fee,
    COALESCE(TRY_CAST(request_fee AS DECIMAL(18, 2)), 0.00) AS request_fee,
    COALESCE(TRY_CAST(discount AS DECIMAL(18, 2)), 0.00) AS discount_amount,
    COALESCE(TRY_CAST(total_fee AS DECIMAL(18, 2)), 0.00) AS total_fee,
    COALESCE(TRY_CAST(distance_price AS DECIMAL(18, 2)), 0.00) AS distance_price,
    COALESCE(TRY_CAST(special_request_price AS DECIMAL(18, 2)), 0.00) AS special_request_price,
    COALESCE(TRY_CAST(stoppoint_price AS DECIMAL(18, 2)), 0.00) AS stoppoint_price,
    COALESCE(TRY_CAST(voucher_discount AS DECIMAL(18, 2)), 0.00) AS voucher_discount,
    COALESCE(TRY_CAST(subtotal_price AS DECIMAL(18, 2)), 0.00) AS subtotal_price,
    COALESCE(TRY_CAST(total_price AS DECIMAL(18, 2)), 0.00) AS total_price,
    COALESCE(TRY_CAST(total_pay AS DECIMAL(18, 2)), 0.00) AS total_pay,
    COALESCE(TRY_CAST(user_main_account AS DECIMAL(18, 2)), 0.00) AS user_main_account,
    COALESCE(TRY_CAST(user_bonus_account AS DECIMAL(18, 2)), 0.00) AS user_bonus_account,
    COALESCE(TRY_CAST(supplier_main_account AS DECIMAL(18, 2)), 0.00) AS supplier_main_account,
    COALESCE(TRY_CAST(supplier_bonus_account AS DECIMAL(18, 2)), 0.00) AS supplier_bonus_account,
    
    -- Ratings & Comments
    COALESCE(TRY_CAST(rating_by_user AS INTEGER), -1) AS rating_by_user,
    COALESCE(CAST(comment_by_user AS VARCHAR), 'NO_NOTE') AS comment_by_user,
    COALESCE(TRY_CAST(rating_by_supplier AS INTEGER), -1) AS rating_by_supplier,
    COALESCE(CAST(comment_by_supplier AS VARCHAR), 'NO_NOTE') AS comment_by_supplier,
    
    -- Additional Info
    COALESCE(CAST(remarks AS VARCHAR), 'NO_NOTE') AS order_remarks,
    COALESCE(TRY_CAST(remind AS BOOLEAN), FALSE) AS is_reminded,
    COALESCE(TRY_CAST("index" AS INTEGER), 0) AS order_index,
    
    -- GeoJSON Extraction
    TRY_CAST(json_extract_scalar(from_location, '$.coordinates[1]') AS DOUBLE) AS from_lat,
    TRY_CAST(json_extract_scalar(from_location, '$.coordinates[0]') AS DOUBLE) AS from_lng,
    
    CAST(requests AS VARCHAR) AS requests_json_array,
    CAST(_rescued_data AS VARCHAR) AS _rescued_data,
    
    -- Metadata
    _ingest_time,
    _run_id,
    current_timestamp AT TIME ZONE 'UTC' AS _silver_updated_at

FROM raw_source
WHERE rn = 1