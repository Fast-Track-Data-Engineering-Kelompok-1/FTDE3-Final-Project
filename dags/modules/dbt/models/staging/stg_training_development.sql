SELECT
    "EmployeeID",
    "Name",
    "TrainingProgram",
    "StartDate",
    "EndDate",
    "Status"
FROM {{ source('data_source', 'kelompok1_training_development') }}