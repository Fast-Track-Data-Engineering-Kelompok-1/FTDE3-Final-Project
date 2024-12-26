{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY pm."EmployeeID", pm."ReviewPeriod") AS performance_key,
    e.employee_key,
    rp.review_period_key,
    pm."Rating" AS Rating
FROM {{ ref('stg_performance_management') }} pm
JOIN {{ ref('dim_employee') }} e ON pm."EmployeeID" = e.EmployeeID
JOIN {{ ref('dim_review_period') }} rp ON pm."ReviewPeriod" = rp.ReviewPeriod