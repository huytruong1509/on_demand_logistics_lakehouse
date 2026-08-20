{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'partner_sk',
    properties = {
        'format': "'PARQUET'"
    },
    tags = ['gold', 'dimension', 'partner']
) }}

WITH source_data AS (
    -- Chuẩn hoá chuỗi và lấy danh sách partner duy nhất từ stg_orders
    SELECT 
        TRIM(partner) AS partner_name,
        MAX(_silver_updated_at) AS _silver_updated_at
    FROM {{ source('silver', 'orders') }}
    WHERE partner IS NOT NULL 
      AND NULLIF(TRIM(partner), '') IS NOT NULL
    {% if is_incremental() %}
        -- Quét delta data dựa trên watermark lookback
        AND _silver_updated_at >= (
            SELECT MAX(_silver_updated_at) - INTERVAL '{{ var("lookback_days", 1) }}' DAY 
            FROM {{ this }}
        )
    {% endif %}
    GROUP BY 1
),

staged AS (
    -- Compute Surrogate Key một lần duy nhất tại CTE
    SELECT
        {{ dbt_utils.generate_surrogate_key(['partner_name']) }} AS partner_sk,
        partner_name,
        _silver_updated_at
    FROM source_data
)

SELECT 
    s.partner_sk,
    s.partner_name,
    
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
    ON t.partner_sk = s.partner_sk
{% endif %}