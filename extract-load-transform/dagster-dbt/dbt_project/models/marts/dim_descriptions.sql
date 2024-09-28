{{ config(materialized='table') }}

SELECT DISTINCT
    id as coin_id,
    catalog,
    description,
    inscriptions,
    txt
FROM {{ ref('stg_roman_coins') }}