{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY td."EmployeeID", td."TrainingProgram", td."StartDate") AS training_key,
    e.employee_key,
    tp.training_program_key,
    start_date.date_key as start_date_key,
    end_date.date_key as end_date_key,
    td."Status" AS Status
FROM {{ ref('stg_training_development') }} td
JOIN {{ ref('dim_employee') }} e ON td."EmployeeID" = e.EmployeeID
JOIN {{ ref('dim_training_program') }} tp ON td."TrainingProgram" = tp.TrainingProgram
LEFT JOIN {{ ref('dim_date') }} start_date ON CAST(td."StartDate" AS DATE) = start_date.date_day
LEFT JOIN {{ ref('dim_date') }} end_date ON CAST(td."EndDate" AS DATE) = end_date.date_day