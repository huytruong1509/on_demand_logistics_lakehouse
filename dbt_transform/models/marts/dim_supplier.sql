{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'supplier_sk',
    properties = {
        'format': "'PARQUET'"
    },
    tags = ['gold', 'dimension', 'supplier']
) }}

WITH raw_source AS (
    -- 1. Lọc và làm sạch dữ liệu từ Silver
    SELECT 
        TRIM(source_system) AS source_system, 
        db_id, 
        TRIM(supplier_id) AS supplier_id, 
        TRIM(supplier_name) AS supplier_name,
        _silver_updated_at
    FROM {{ source('silver', 'orders') }}
    WHERE supplier_id IS NOT NULL 
      AND TRIM(supplier_id) NOT IN ('', 'UNKNOWN')
    {% if is_incremental() %}
        -- Quét delta data
        AND _silver_updated_at >= (
            SELECT MAX(_silver_updated_at) - INTERVAL '{{ var("lookback_days", 1) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
),

deduped_source AS (
    -- 2. Deduplication chuẩn SCD Type 1: Chỉ lấy thông tin mới nhất của Supplier nếu có thay đổi tên
    SELECT 
        source_system,
        db_id,
        supplier_id,
        supplier_name,
        _silver_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY source_system, db_id, supplier_id 
            ORDER BY _silver_updated_at DESC
        ) AS rn
    FROM raw_source
),

staged AS (
    -- 3. Tạo Surrogate Key 1 lần duy nhất
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['source_system', 'db_id', 'supplier_id']) }} AS supplier_sk,
        source_system,
        db_id,
        supplier_id,
        supplier_name,
        _silver_updated_at
    FROM deduped_source
    WHERE rn = 1
)

SELECT 
    s.supplier_sk,
    s.source_system,
    s.db_id,
    s.supplier_id,
    s.supplier_name,
    
    -- 4. Audit & Timestamps
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
    ON t.supplier_sk = s.supplier_sk
{% endif %}