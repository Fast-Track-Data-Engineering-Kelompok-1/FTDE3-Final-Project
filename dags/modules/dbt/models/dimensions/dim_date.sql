{{ config(materialized='table', unique_key='date_key') }}

WITH date_series AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2024-12-31' as date)"
    ) }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY date_day) AS date_key,  -- Generate the date_key
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day
FROM date_series