{{ config(materialized='table') }}

SELECT *
FROM {{ ref('match_transforms') }}
WHERE Rank_1 BETWEEN 1 AND 10
