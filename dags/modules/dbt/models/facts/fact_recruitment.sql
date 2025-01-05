{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY rs."CandidateID") AS recruitment_key,
    c.candidate_key,
    app_date.date_key AS application_date_key,
    int_date.date_key AS interview_date_key,
    rs."OfferStatus" AS OfferStatus,
    rs."Predict" AS Predict
FROM {{ ref('stg_recruitment_selection') }} rs
JOIN {{ ref('dim_candidate') }} c ON rs."CandidateID" = c.CandidateID
LEFT JOIN {{ ref('dim_date') }} app_date ON CAST(rs."ApplicationDate" AS DATE) = app_date.date_day
LEFT JOIN {{ ref('dim_date') }} int_date ON CAST(rs."InterviewDate" AS DATE) = int_date.date_day