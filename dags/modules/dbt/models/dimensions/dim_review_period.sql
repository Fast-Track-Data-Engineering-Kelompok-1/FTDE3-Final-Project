{{ config(unique_key='review_period_key') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "ReviewPeriod") AS review_period_key,
    "ReviewPeriod" AS ReviewPeriod
FROM {{ ref('stg_performance_management') }}
GROUP BY "ReviewPeriod"