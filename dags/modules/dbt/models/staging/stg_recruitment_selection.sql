SELECT
    "CandidateID",
    "Name",
    "Gender",
    "Age",
    "Position",
    "ApplicationDate",
    "Status",
    "InterviewDate",
    "OfferStatus"
FROM {{ source('data_source', 'kelompok1_recruitment_selection') }}