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

  {# if no tests were run, handle deactivation for current models and return #}
  {% if test_results | length == 0 -%}
    {% set current_models = [] %}
    {%- for result in results -%}
      {%- if result.node.resource_type == 'model' -%}
        {%- do current_models.append(result.node.name) -%}
      {%- endif -%}
    {%- endfor -%}

    {% if current_models | length > 0 and execute %}
      {% set deactivation_query %}
        update {{ target_schema }}.dq_dbt_test_results_central
        set is_active = 'N',
            deactivated_timestamp = convert_timezone('America/New_York', current_timestamp::timestamp_ntz)
        where is_active = 'Y'
        and (
          {%- for model in current_models -%}
            model_refs like '%{{ model }}%'
            {%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
        )
      {% endset %}
      {% do run_query(deactivation_query) %}
    {% endif %}
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

          {%- set model_info = process_refs(names_list, is_src=false, schemas=node_schemas) -%}
          {%- set source_info = process_refs(result.node.sources, is_src=true, schemas=node_schemas) -%}

          {# Enhanced logic for determining test level and column details using column_name #}
          {%- set test_level = 'table' -%}
          {%- set column_details = '' -%}

          {# Debug logging #}
          {% do log("Test name: " ~ test_name, info=True) %}
          {% do log("Test type: " ~ test_type, info=True) %}
          {% do log("Column name: " ~ result.node.column_name, info=True) %}

          {# First check if column_name exists in the test node #}
          {%- if result.node.column_name is not none -%}
            {%- set test_level = 'column' -%}
            {%- set column_details = result.node.column_name -%}
          
          {# Fallback to previous logic if column_name is None #}
          {%- else -%}
            {# Handle schema.yaml generic tests #}
            {%- if result.node.test_metadata is defined -%}
              {# Check if test has column field in kwargs #}
              {%- if result.node.test_metadata.kwargs.column is defined -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.column -%}
              {# Check if test has columns field in kwargs (for multi-column tests) #}
              {%- elif result.node.test_metadata.kwargs.columns is defined -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.columns | join(',') -%}
              {# Handle relationship tests #}
              {%- elif result.node.test_metadata.name == 'relationships' -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.to -%}
              {%- else -%}
                {# For other generic tests without explicit column info, try to extract from name #}
                {%- if result.node.name is string and '_by_' in result.node.name -%}
                  {%- set test_level = 'column' -%}
                  {%- set column_details = result.node.name.split('_by_')[1] -%}
                {%- endif -%}
              {%- endif -%}
            {%- endif -%}
          {%- endif -%}

          {# Extract rule_level from config meta #}
          {%- set rule_level = result.node.config.meta.rule_level if result.node.config.meta is defined and result.node.config.meta.rule_level is defined else 'Medium' -%}
          
          {# Add rule_level to debug logging #}
          {% do log("Rule Level: " ~ rule_level, info=True) %}

          {# Add detailed debug logging for test result metadata #}
          {% do log("=================== Test Result Debug Info ===================", info=True) %}
          {% do log("Test Full Node: " ~ result.node, info=True) %}
          {% do log("Test Name: " ~ result.node.name, info=True) %}
          {% do log("Resource Type: " ~ result.node.resource_type, info=True) %}
          {% do log("Test Status: " ~ result.status, info=True) %}
          {% do log("Execution Time: " ~ result.execution_time, info=True) %}
          {% do log("Test Metadata: " ~ result.node.test_metadata, info=True) %}
          {% do log("Column Name: " ~ result.node.column_name, info=True) %}
          {% do log("Config: " ~ result.node.config, info=True) %}
          {% do log("Original File Path: " ~ result.node.original_file_path, info=True) %}
          {% do log("Compiled SQL: " ~ result.node.compiled_code, info=True) %}
          {% do log("References: " ~ result.node.refs, info=True) %}
          {% do log("Sources: " ~ result.node.sources, info=True) %}
          {% do log("========================================================", info=True) %}

          select
            {{ dbt_utils.generate_surrogate_key(["'" ~ test_name ~ "'", "current_time"]) }} as test_sk,
            '{{ test_name }}'::varchar(4000) as test_name,
            '{{ result.node.name }}'::varchar(10000) as test_name_long,
            '{{ test_type }}'::varchar(250) as test_type,
            '{{ test_level }}'::varchar(50) as test_level,
            '{{ column_details }}'::varchar(1000) as column_details,
            '{{ model_info }}'::varchar(10000) as model_refs,
            split_part('{{ model_info }}', '.', 1)::varchar(255) as model_schema,
            split_part('{{ model_info }}', '.', 2)::varchar(255) as model_table,
            '{{ source_info }}'::varchar(10000) as source_refs,
            '{{ rule_level }}'::varchar(50) as rule_level,
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
        test_sk, test_name, test_name_long, test_type, test_level, 
        column_details, model_refs, model_schema, model_table, source_refs,
        rule_level, test_severity_config, execution_time_seconds, test_result, 
        file_test_defined, compiled_sql, TIMESTAMP, is_active, 
        deactivated_timestamp
      )
      values (
        source.test_sk, source.test_name, source.test_name_long, 
        source.test_type, source.test_level, source.column_details,
        source.model_refs, source.model_schema, source.model_table,
        source.source_refs, source.rule_level, source.test_severity_config,
        source.execution_time_seconds, source.test_result, 
        source.file_test_defined, source.compiled_sql, source.TIMESTAMP,
        'Y', null
      );

    -- Get current models and tests being tested
    {% set current_models_query %}
      select distinct 
        model_name,
        test_name,
        test_path,
        test_type
      from (
        with current_models as (
          {%- for result in test_results -%}
            {%- set names_list = [] -%}
            {%- for ref in result.node.refs -%}
              {%- do names_list.append([ref.name]) -%}
            {%- endfor -%}
            select 
              '{{ process_refs(names_list, is_src=false, schemas=node_schemas) }}' as model_refs,
              '{{ result.node.name }}' as test_name,
              '{{ result.node.original_file_path }}' as test_path,
              '{{ 'generic' if result.node.test_metadata is defined else 'singular' }}' as test_type
            {{ "union all " if not loop.last }}
          {%- endfor -%}
        )
        select 
          value::varchar as model_name,
          test_name,
          test_path,
          test_type
        from current_models,
        lateral flatten(input => split(model_refs, ','))
        where model_refs != ''
      )
    {% endset %}

    -- Enhanced deactivation logic with better singular test handling
    update {{ central_tbl }}
    set is_active = 'N',
        deactivated_timestamp = convert_timezone('America/New_York', current_timestamp::timestamp_ntz)
    where is_active = 'Y'
    and (
      exists (
        select 1 
        from ({{ current_models_query }}) as cm
        where (
          -- For generic tests, check model references
          ({{ central_tbl }}.test_type = 'generic' 
           and {{ central_tbl }}.model_refs like '%' || cm.model_name || '%')
          -- For singular tests, check test name and path
          or ({{ central_tbl }}.test_type = 'singular' 
              and {{ central_tbl }}.test_name = cm.test_name
              and {{ central_tbl }}.file_test_defined = cm.test_path)
        )
      )
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

          {%- set model_info = process_refs(names_list, is_src=false, schemas=node_schemas) -%}
          {%- set source_info = process_refs(result.node.sources, is_src=true, schemas=node_schemas) -%}

          {# Enhanced logic for determining test level and column details using column_name #}
          {%- set test_level = 'table' -%}
          {%- set column_details = '' -%}

          {# Debug logging #}
          {% do log("Test name: " ~ test_name, info=True) %}
          {% do log("Test type: " ~ test_type, info=True) %}
          {% do log("Column name: " ~ result.node.column_name, info=True) %}

          {# First check if column_name exists in the test node #}
          {%- if result.node.column_name is not none -%}
            {%- set test_level = 'column' -%}
            {%- set column_details = result.node.column_name -%}
          
          {# Fallback to previous logic if column_name is None #}
          {%- else -%}
            {# Handle schema.yaml generic tests #}
            {%- if result.node.test_metadata is defined -%}
              {# Check if test has column field in kwargs #}
              {%- if result.node.test_metadata.kwargs.column is defined -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.column -%}
              {# Check if test has columns field in kwargs (for multi-column tests) #}
              {%- elif result.node.test_metadata.kwargs.columns is defined -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.columns | join(',') -%}
              {# Handle relationship tests #}
              {%- elif result.node.test_metadata.name == 'relationships' -%}
                {%- set test_level = 'column' -%}
                {%- set column_details = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.to -%}
              {%- else -%}
                {# For other generic tests without explicit column info, try to extract from name #}
                {%- if result.node.name is string and '_by_' in result.node.name -%}
                  {%- set test_level = 'column' -%}
                  {%- set column_details = result.node.name.split('_by_')[1] -%}
                {%- endif -%}
              {%- endif -%}
            {%- endif -%}
          {%- endif -%}

          {# Extract rule_level from config meta #}
          {%- set rule_level = result.node.config.meta.rule_level if result.node.config.meta is defined and result.node.config.meta.rule_level is defined else 'Low' -%}
          
          {# Add rule_level to debug logging #}
          {% do log("Rule Level: " ~ rule_level, info=True) %}

          {# Add detailed debug logging for test result metadata #}
          {% do log("=================== Test Result Debug Info ===================", info=True) %}
          {% do log("Test Full Node: " ~ result.node, info=True) %}
          {% do log("Test Name: " ~ result.node.name, info=True) %}
          {% do log("Resource Type: " ~ result.node.resource_type, info=True) %}
          {% do log("Test Status: " ~ result.status, info=True) %}
          {% do log("Execution Time: " ~ result.execution_time, info=True) %}
          {% do log("Test Metadata: " ~ result.node.test_metadata, info=True) %}
          {% do log("Column Name: " ~ result.node.column_name, info=True) %}
          {% do log("Config: " ~ result.node.config, info=True) %}
          {% do log("Original File Path: " ~ result.node.original_file_path, info=True) %}
          {% do log("Compiled SQL: " ~ result.node.compiled_code, info=True) %}
          {% do log("References: " ~ result.node.refs, info=True) %}
          {% do log("Sources: " ~ result.node.sources, info=True) %}
          {% do log("========================================================", info=True) %}

          select
            {{ dbt_utils.generate_surrogate_key(["'" ~ test_name ~ "'", "current_time"]) }} as test_sk,
            '{{ test_name }}'::varchar(4000) as test_name,
            '{{ result.node.name }}'::varchar(10000) as test_name_long,
            '{{ test_type }}'::varchar(250) as test_type,
            '{{ test_level }}'::varchar(50) as test_level,
            '{{ column_details }}'::varchar(1000) as column_details,
            '{{ model_info }}'::varchar(10000) as model_refs,
            split_part('{{ model_info }}', '.', 1)::varchar(255) as model_schema,
            split_part('{{ model_info }}', '.', 2)::varchar(255) as model_table,
            '{{ source_info }}'::varchar(10000) as source_refs,
            '{{ rule_level }}'::varchar(50) as rule_level,
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
  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ return(ref|join('.')) }}
        {% else %}
          {% if schemas %}
            {{ return(schemas[ref[0]]~'.'~ref[0]) }}
          {% else %}
            {{ return(ref[0]) }}
          {% endif %}
        {% endif %} 
      {% endfor %}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}