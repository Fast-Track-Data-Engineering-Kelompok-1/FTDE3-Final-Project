{{ config( unique_key='candidate_key') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "CandidateID") as candidate_key,
    "CandidateID" AS CandidateID,
    "Name" AS Name,
    "Gender" AS Gender,
    "Age" AS Age,
    "Position" AS Position
FROM {{ ref('stg_recruitment_selection') }}