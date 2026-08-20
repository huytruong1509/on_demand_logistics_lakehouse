{% macro generate_dates_dimension(start_date) %}

/*
 - Trino Version: Generate Date Spine
 - Assuming start of fiscal year as at 1st of July.
 - Automatically generate dates up to 12 months in the future.
*/

WITH sequence_generator AS (
    -- Xác định ngày bắt đầu và ngày kết thúc (12 tháng sau so với hiện tại)
    SELECT 
        CAST('{{ start_date }}' AS DATE) AS start_d,
        current_date + INTERVAL '12' MONTH AS end_d
),

dates AS (
    -- Trino bung mảng sequence thành các dòng tự động (thay thế GENERATOR/SEQ4)
    SELECT CAST(d AS DATE) AS date
    FROM sequence_generator
    CROSS JOIN UNNEST(sequence(start_d, end_d, INTERVAL '1' DAY)) AS t(d)
),

dates_fin AS (
    SELECT 
        date AS date_id,
        day_of_week(date) AS day_of_week, -- 1=Monday, 7=Sunday
        date_format(date, '%a') AS day_of_week_name,
        date_trunc('week', date) AS cal_week_start_date,
        EXTRACT(DAY FROM date) AS day_of_month,
        EXTRACT(MONTH FROM date) AS cal_month,
        date_format(date, '%M') AS cal_mon_name,
        date_format(date, '%b') AS cal_mon_name_short,
        EXTRACT(QUARTER FROM date) AS cal_quarter,
        CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS VARCHAR)) AS cal_quarter_name,
        EXTRACT(YEAR FROM date) AS cal_year,
        
        -- Logic cuối tuần của Trino
        CASE WHEN day_of_week(date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        
        -- Năm tài chính (Bắt đầu từ tháng 7)
        CASE 
            WHEN EXTRACT(MONTH FROM date) < 7 THEN EXTRACT(YEAR FROM date)
            ELSE EXTRACT(YEAR FROM date) + 1
        END AS fin_year,
        
        CASE 
            WHEN EXTRACT(MONTH FROM date) < 7 THEN EXTRACT(MONTH FROM date) + 6
            ELSE EXTRACT(MONTH FROM date) - 6
        END AS fin_period,
        
        CASE 
            WHEN EXTRACT(MONTH FROM date) < 7 THEN EXTRACT(QUARTER FROM date) + 2
            ELSE EXTRACT(QUARTER FROM date) - 2
        END AS fin_quarter,
        
        CASE
            WHEN date < date_trunc('year', date) + INTERVAL '6' MONTH 
            THEN EXTRACT(WEEK FROM (date - INTERVAL '6' MONTH))
            ELSE EXTRACT(WEEK FROM (date + INTERVAL '6' MONTH))
        END AS fin_week
    FROM dates
)

SELECT 
    *,
    CONCAT('p', CAST(fin_period AS VARCHAR)) AS fin_period_name,
    CONCAT('FQ', CAST(fin_quarter AS VARCHAR)) AS fin_quarter_name,
    CONCAT('wk', CAST(fin_week AS VARCHAR)) AS fin_week_name
FROM dates_fins

{% endmacro %}