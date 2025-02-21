-- create some structure tablle for markets in dbt select statement.

{{ config(materialized='table',
tags=['markets']

) }}

with source_data as (
    select 1 as id
    union all
    select null as id
)
select 
    id,
    'value_1' as column_1,
    'value_2' as column_2,
    'value_3' as column_3
from source_data