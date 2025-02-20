
-- Use the `ref` function to select from other models
{{ config(materialized='table',
tags=['example_2']

) }}
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
