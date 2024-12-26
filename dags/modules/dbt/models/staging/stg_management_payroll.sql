SELECT
    "EmployeeID",
    "Name",
    "Gender",
    "Age",
    "Department",
    "Position",
    "Salary",
    "OvertimePay",
    "PaymentDate"
FROM {{ source('data_source', 'kelompok1_management_payroll') }}