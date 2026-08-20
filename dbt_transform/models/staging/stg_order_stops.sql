{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['source_system', 'db_id', 'order_id', 'stop_index'],
    properties = {
        'format': "'PARQUET'",
        'partitioning': "ARRAY['day(create_time)']"
    },
    tags = ['silver', 'stops']
) }}

WITH filtered_source AS (
    -- Bước 1: Lọc data, áp dụng incremental logic sớm nhất có thể để giảm I/O
    SELECT 
        CAST(_id AS VARCHAR) AS order_id,
        CAST(_db_id AS BIGINT) AS db_id,
        CAST(_source_system AS VARCHAR) AS source_system,
        CAST(create_time AS DOUBLE) AS raw_create_time,
        CAST(path AS VARCHAR) AS raw_path_json,
        _ingest_time
    FROM {{ source('bronze_logistics', 'raw_orders') }}
    WHERE path IS NOT NULL AND TRIM(path) NOT IN ('', '[]')
    {% if is_incremental() %}
        AND _ingest_time >= (SELECT MAX(_ingest_time) - INTERVAL '{{ var("lookback_days", 1) }}' DAY FROM {{ this }})
    {% endif %}
),

deduped_orders AS (
    -- Bước 2: Sử dụng max_by thay vì ROW_NUMBER để chống tràn RAM (OOM) 
    SELECT 
        source_system,
        db_id,
        order_id,
        MAX(_ingest_time) AS _ingest_time,
        -- Lấy create_time và JSON path của bản ghi có _ingest_time mới nhất
        max_by(raw_create_time, _ingest_time) AS raw_create_time,
        max_by(raw_path_json, _ingest_time) AS raw_path_json
    FROM filtered_source
    GROUP BY 1, 2, 3
),

parsed_json AS (
    -- Bước 3: Chỉ Parse JSON trên tập data đã được deduplicate
    SELECT 
        order_id,
        db_id,
        source_system,
        TRY(from_unixtime(raw_create_time)) AT TIME ZONE 'UTC' AS create_time,
        raw_path_json,
        _ingest_time,
        TRY(CAST(json_parse(raw_path_json) AS ARRAY(ROW(
            address VARCHAR, lat DOUBLE, lng DOUBLE, name VARCHAR, mobile VARCHAR, 
            remarks VARCHAR, building VARCHAR, apt_number VARCHAR, status VARCHAR, 
            complete_time DOUBLE, complete_lat DOUBLE, complete_lng DOUBLE, 
            complete_comment VARCHAR, image_url VARCHAR, pod_info VARCHAR, 
            rating_by_receiver INTEGER, comment_by_receiver VARCHAR, fail_time DOUBLE, 
            fail_lat DOUBLE, fail_lng DOUBLE, fail_comment VARCHAR, 
            redelivery_note VARCHAR, cod DOUBLE, tracking_number VARCHAR, 
            require_pod BOOLEAN, require_verification BOOLEAN
        )))) AS stops_array
    FROM deduped_orders
)

SELECT 
    bo.source_system,
    bo.db_id,
    bo.order_id,
    COALESCE(t.stop_index, 0) AS stop_index,
    bo.create_time,
    
    t.name AS stop_name,
    t.mobile AS stop_mobile,
    t.address AS stop_address,
    t.building AS building,
    t.apt_number AS apt_number,
    
    t.lat AS stop_lat,
    t.lng AS stop_lng,
    t.complete_lat AS complete_lat,
    t.complete_lng AS complete_lng,
    t.fail_lat AS fail_lat,
    t.fail_lng AS fail_lng,
    
    -- Group 2: Tài chính -> COALESCE về 0.0
    COALESCE(TRY_CAST(t.cod AS DECIMAL(18, 2)), 0.00) AS cod_amount,
    UPPER(TRIM(t.status)) AS stop_status,
    t.tracking_number AS tracking_number,
    
    -- Group 1: Categorical / Notes -> COALESCE về NO_NOTE
    COALESCE(CAST(t.remarks AS VARCHAR), 'NO_NOTE') AS stop_remarks,
    COALESCE(CAST(t.redelivery_note AS VARCHAR), 'NO_NOTE') AS redelivery_note,
    
    COALESCE(t.require_pod, FALSE) AS is_pod_required,
    COALESCE(t.require_verification, FALSE) AS is_verification_required,
    t.pod_info AS pod_info,
    t.image_url AS pod_image_url,
    
    -- Group 2 (Rating): Khác biệt -> COALESCE về -1
    COALESCE(t.rating_by_receiver, -1) AS rating_by_receiver,
    COALESCE(CAST(t.comment_by_receiver AS VARCHAR), 'NO_NOTE') AS comment_by_receiver,
    COALESCE(CAST(t.complete_comment AS VARCHAR), 'NO_NOTE') AS complete_comment,
    COALESCE(CAST(t.fail_comment AS VARCHAR), 'NO_NOTE') AS fail_comment,
    
    -- Group 3: Timestamps -> Giữ nguyên NULL
    TRY(from_unixtime(t.complete_time)) AT TIME ZONE 'UTC' AS complete_time,
    TRY(from_unixtime(t.fail_time)) AT TIME ZONE 'UTC' AS fail_time,
    
    CASE 
        WHEN bo.raw_path_json IS NOT NULL AND bo.stops_array IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_parse_failed,
    
    bo._ingest_time,
    current_timestamp AT TIME ZONE 'UTC' AS _silver_updated_at

FROM parsed_json bo
LEFT JOIN UNNEST(bo.stops_array) WITH ORDINALITY AS t(
    address, lat, lng, name, mobile, 
    remarks, building, apt_number, status, 
    complete_time, complete_lat, complete_lng, 
    complete_comment, image_url, pod_info, 
    rating_by_receiver, comment_by_receiver, fail_time, 
    fail_lat, fail_lng, fail_comment, 
    redelivery_note, cod, tracking_number, 
    require_pod, require_verification, 
    stop_index
) ON TRUE