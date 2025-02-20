
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',
tags=['example_1']

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

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
