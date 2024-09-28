{{ config(materialized='table') }}

SELECT id, 
name as ruler, 
name_detail as ruler_info, 
catalog, 
description, 
metal, 
mass, 
diameter, 
era,
year, 
inscriptions, 
txt, 
created, 
modified
FROM {{ source("main", "raw_roman_coins") }}