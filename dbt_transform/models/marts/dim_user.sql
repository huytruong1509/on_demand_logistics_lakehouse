{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'user_sk',
    properties = {
        'format': "'PARQUET'"
    },
    tags = ['gold', 'dimension', 'user']
) }}

WITH raw_source AS (
    -- 1. Làm sạch và lấy dữ liệu thô từ Silver
    SELECT 
        TRIM(source_system) AS source_system, 
        db_id, 
        TRIM(user_id) AS user_id, 
        TRIM(user_name) AS user_name,
        _silver_updated_at
    FROM {{ source('silver', 'orders') }}
    WHERE user_id IS NOT NULL 
      AND TRIM(user_id) NOT IN ('', 'UNKNOWN')
    {% if is_incremental() %}
        -- Quét delta data dựa trên watermark
        AND _silver_updated_at >= (
            SELECT MAX(_silver_updated_at) - INTERVAL '{{ var("lookback_days", 1) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
),

deduped_source AS (
    -- 2. Deduplication (SCD Type 1): Luôn lấy thông tin (tên) cập nhật mới nhất của User
    SELECT 
        source_system,
        db_id,
        user_id,
        user_name,
        _silver_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY source_system, db_id, user_id 
            ORDER BY _silver_updated_at DESC
        ) AS rn
    FROM raw_source
),

staged AS (
    -- 3. Tạo Surrogate Key 1 lần duy nhất để tối ưu hiệu năng
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'user_id']) }} AS user_sk,
        source_system,
        db_id,
        user_id,
        user_name,
        _silver_updated_at
    FROM deduped_source
    WHERE rn = 1
)

SELECT 
    s.user_sk,
    s.source_system,
    s.db_id,
    s.user_id,
    s.user_name,
    
    -- 4. Audit columns
    {% if is_incremental() %}
        COALESCE(t.created_at, CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE)) AS created_at,
    {% else %}
        CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS created_at,
    {% endif %}
    
    CAST({{ dbt.current_timestamp() }} AS TIMESTAMP(6) WITH TIME ZONE) AS updated_at,
    s._silver_updated_at

FROM staged s
{% if is_incremental() %}
LEFT JOIN {{ this }} t 
    ON t.user_sk = s.user_sk
{% endif %}