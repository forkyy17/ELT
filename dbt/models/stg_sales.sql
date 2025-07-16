{{ config(materialized='table') }}

select year, anzsic_descriptor, data_value
from 
{{ source('sales_source', 'sales') }}