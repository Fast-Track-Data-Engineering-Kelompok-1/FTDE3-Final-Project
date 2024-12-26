{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY p."PaymentDate") as payroll_key,
    e.employee_key,
    d.date_key,
    p."Salary" AS Salary,
    p."OvertimePay" AS OvertimePay
FROM {{ ref('stg_management_payroll') }} p
JOIN {{ ref('dim_employee') }} e ON p."EmployeeID" = e.EmployeeID
JOIN {{ ref('dim_date') }} d ON CAST(p."PaymentDate" AS DATE) = d.date_day