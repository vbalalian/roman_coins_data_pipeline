{{ config(materialized='table') }}

SELECT DISTINCT
    ruler,
    ruler_info
FROM {{ ref ('stg_roman_coins') }}