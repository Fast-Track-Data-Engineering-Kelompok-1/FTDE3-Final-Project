{{ config(unique_key='training_program_key') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "TrainingProgram") AS training_program_key,
    "TrainingProgram" AS TrainingProgram
FROM {{ ref('stg_training_development') }}
GROUP BY "TrainingProgram" -- Deduplicate training programs