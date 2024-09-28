{{ config(materialized='table') }}

SELECT DISTINCT
    id,
    ruler,
    metal,
    mass,
    diameter,
    year,
    created, 
    modified
FROM {{ ref('stg_roman_coins') }}