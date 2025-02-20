{% macro store_test_results(results, store_schema=false) %}

{# Set current_time using Jinja's datetime module #}
{% set current_time = modules.datetime.datetime.now().isoformat() %}

{%- if 1==1 -%}
{% set target_schema = 'dqm' %}

  {%- set central_tbl -%} {{ target_schema }}.dq_dbt_test_results_central {%- endset -%}
  {%- set test_results = [] -%}
  {%- for result in results if result.node.resource_type == 'test' -%}
    {%- do test_results.append(result) -%}
  {%- endfor -%}

  {# if no tests were run, skip this iteration #}
  {% if test_results | length == 0 -%}
    {{ return('select 1') }}
  {%- endif -%}

  

  {# Parse graph an get schema info for all models/sources #}
  {% if store_schema %}
    {% set node_schemas = get_node_schemas() %}
  {% else %}
    {% set node_schemas = {} %}
  {% endif %}

  {# Check if table exists #}
  {% set central_table_query %} {{ dbt_utils.get_tables_by_pattern_sql(target_schema | upper, 'DQ_DBT_TEST_RESULTS_CENTRAL') }} {% endset %}
  {% if execute %}
    {% set central_table_exists = run_query(central_table_query) %}
  {% endif %}

  {% if central_table_exists %}
    {% set current_test_runs_query %}
      with current_test_runs as (
        {%- for result in test_results -%}
          {%- set test_name = result.node.test_metadata.name if result.node.test_metadata is defined else result.node.name -%}
          select
            '{{ test_name }}'::varchar(4000) as test_name,
            '{{ result.node.compiled_code | replace("\n", " ") | replace("'","''") | trim }}'::varchar(65535) as compiled_sql
          {{ "union all " if not loop.last }}
        {%- endfor -%}
      )
      select * from current_test_runs
    {% endset %}

    merge into {{ central_tbl }} as target using (
      with current_test_runs as (
        {%- for result in test_results -%}
          {%- set test_name = result.node.test_metadata.name if result.node.test_metadata is defined else result.node.name -%}
          {%- set test_type = 'generic' if result.node.test_metadata is defined else 'singular' -%}
          
          {%- set names_list = [] -%}
          {%- for ref in result.node.refs -%}
            {%- do names_list.append([ref.name]) -%}
          {%- endfor -%}

          select
            {{ dbt_utils.generate_surrogate_key(["'" ~ test_name ~ "'", "current_time"]) }} as test_sk,
            '{{ test_name }}'::varchar(4000) as test_name,
            '{{ result.node.name }}'::varchar(10000) as test_name_long,
            '{{ test_type }}'::varchar(250) as test_type,
            '{{ process_refs(names_list, is_src=false, schemas=node_schemas) }}'::varchar(10000) as model_refs,
            '{{ process_refs(result.node.sources, is_src=true, schemas=node_schemas) }}'::varchar(10000) as source_refs,
            '{{ result.node.config.severity }}'::varchar(250) as test_severity_config,
            '{{ result.execution_time }}'::text as execution_time_seconds,
            '{{ result.status }}'::varchar(125) as test_result,
            '{{ result.node.original_file_path }}'::varchar(4000) as file_test_defined,
            '{{ result.node.compiled_code | replace("\n", " ") | replace("'","''") | trim }}'::varchar(65535) as compiled_sql,
            convert_timezone('America/New_York', current_timestamp::timestamp_ntz) as TIMESTAMP
          {{ "union all " if not loop.last }}
        {%- endfor -%}
      )
      select 
        s.*,
        'Y' as is_active,
        null as deactivated_timestamp
      from current_test_runs s
    ) as source 
      on target.test_name = source.test_name 
      and target.compiled_sql = source.compiled_sql
    when matched then
      update set 
        TIMESTAMP = convert_timezone('America/New_York', current_timestamp::timestamp_ntz),
        is_active = 'Y',
        deactivated_timestamp = null
    when not matched then
      insert (
        test_sk, test_name, test_name_long, test_type, model_refs,
        source_refs, test_severity_config, execution_time_seconds,
        test_result, file_test_defined, compiled_sql, TIMESTAMP,
        is_active, deactivated_timestamp
      )
      values (
        source.test_sk, source.test_name, source.test_name_long, 
        source.test_type, source.model_refs, source.source_refs,
        source.test_severity_config, source.execution_time_seconds,
        source.test_result, source.file_test_defined,
        source.compiled_sql, source.TIMESTAMP,
        'Y', null
      );

    -- Get current models being tested
    {% set current_models_query %}
      with current_models as (
        {%- for result in test_results -%}
          {%- set names_list = [] -%}
          {%- for ref in result.node.refs -%}
            {%- do names_list.append([ref.name]) -%}
          {%- endfor -%}
          select '{{ process_refs(names_list, is_src=false, schemas=node_schemas) }}' as model_refs
          {{ "union all " if not loop.last }}
        {%- endfor -%}
      ),
      split_models as (
        select distinct value as model_name
        from current_models,
        lateral flatten(input => split(model_refs, ','))
        where model_refs != ''
      )
      select * from split_models
    {% endset %}

    -- Handle deactivation of removed tests, but only for models currently being tested
    update {{ central_tbl }}
    set is_active = 'N',
        deactivated_timestamp = convert_timezone('America/New_York', current_timestamp::timestamp_ntz)
    where is_active = 'Y'
    and exists (
      select 1 
      from ({{ current_models_query }}) as current_models
      where {{ central_tbl }}.model_refs like '%' || current_models.model_name || '%'
    )
    and not exists (
      select 1 
      from ({{ current_test_runs_query }}) as current_tests
      where current_tests.test_name = {{ central_tbl }}.test_name
      and current_tests.compiled_sql = {{ central_tbl }}.compiled_sql
    );

  {% else %}
    create table {{ central_tbl }} as (
      with current_test_runs as (
        {%- for result in test_results -%}
          {%- set test_name = result.node.test_metadata.name if result.node.test_metadata is defined else result.node.name -%}
          {%- set test_type = 'generic' if result.node.test_metadata is defined else 'singular' -%}
          
          {%- set names_list = [] -%}
          {%- for ref in result.node.refs -%}
            {%- do names_list.append([ref.name]) -%}
          {%- endfor -%}

          select
            {{ dbt_utils.generate_surrogate_key(["'" ~ test_name ~ "'", "current_time"]) }} as test_sk,
            '{{ test_name }}'::varchar(4000) as test_name,
            '{{ result.node.name }}'::varchar(10000) as test_name_long,
            '{{ test_type }}'::varchar(250) as test_type,
            '{{ process_refs(names_list, is_src=false, schemas=node_schemas) }}'::varchar(10000) as model_refs,
            '{{ process_refs(result.node.sources, is_src=true, schemas=node_schemas) }}'::varchar(10000) as source_refs,
            '{{ result.node.config.severity }}'::varchar(250) as test_severity_config,
            '{{ result.execution_time }}'::text as execution_time_seconds,
            '{{ result.status }}'::varchar(125) as test_result,
            '{{ result.node.original_file_path }}'::varchar(4000) as file_test_defined,
            '{{ result.node.compiled_code | replace("\n", " ") | replace("'","''") | trim }}'::varchar(65535) as compiled_sql,
            convert_timezone('America/New_York', current_timestamp::timestamp_ntz) as TIMESTAMP
          {{ "union all " if not loop.last }}
        {%- endfor -%}
      )
      select 
        s.*,
        'Y' as is_active,
        null as deactivated_timestamp
      from current_test_runs s
    )
  {% endif %}
{%- endif -%}  

{% endmacro %}


/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders
  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/
{% macro process_refs( ref_list, is_src=false, schemas=None ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {% if schemas %}
            {% set ref_cleaned = schemas[ref[0]]~'.'~ref[0] %}
          {% else %}
            {% set ref_cleaned = ref[0] %}
          {% endif %}
          {{ refs.append(ref_cleaned) }}
        {% endif %} 
      {% endfor %}

      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}