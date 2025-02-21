
-- Use the `ref` function to select from other models
{{ config(materialized='table',
tags=['fact_sales']

) }}
select *, 'value_4' as column_4
from {{ ref('fact_orders') }}
where id = 1
