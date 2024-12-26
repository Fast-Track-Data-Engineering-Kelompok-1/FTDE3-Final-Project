SELECT
    "EmployeeID",
    "Name",
    "ReviewPeriod",
    "Rating",
    "Comments"
FROM {{ source('data_source', 'kelompok1_performance_management') }}