{{ config(unique_key='employee_key') }}

WITH employee_data AS (
    SELECT
        "EmployeeID",
        "Name",
        "Gender",
        "Age",
        "Department",
        "Position"
    FROM {{ ref('stg_management_payroll') }}
),
ranked_employees AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY "EmployeeID") as rn
    FROM employee_data
)
SELECT
    rn AS employee_key,
    "EmployeeID" AS EmployeeID,
    "Name" AS Name,
    "Gender" AS Gender,
    "Age" AS Age,
    "Department" AS Department,
    "Position" AS Position
FROM ranked_employees