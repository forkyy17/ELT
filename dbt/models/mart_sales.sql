{{ config(materialized='table') }}
select year, data_value
FROM {{ ref('stg_sales') }}
