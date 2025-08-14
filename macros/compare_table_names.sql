{% macro get_model_name_mismatches_sql(database, schema, table_name, column_name, only_this_project=True, include_disabled=False, case_sensitive=False, direction='both') %}
--   This macro compares all dbt model names against a list of names in a db table
--   Database, schema, table_name, column_name parameters point to the location of the table and column containing the list to compare against
--   Only_this_project and include_disabled control scope of models compared (defaults True, False)
--   Case sensitive (default False) compares names with oroginal cases if True
--   Direction (default 'both') compares direction of comparison.  Options are:
--      - 'source' - identifies missing models that are listed in the table
--      - 'model' - identifies existing models that are missing in the table
  
  {# First get source values #}
  {% set source_query %}
    select distinct 
    {% if case_sensitive %}
      {{ adapter.quote(column_name) }} as name
    {% else %}
      lower({{ adapter.quote(column_name) }}) as name
    {% endif %}
    from {{ adapter.quote(database) ~ '.' ~ adapter.quote(schema) ~ '.' ~ adapter.quote(table_name) }}
  {% endset %}

  {% if execute %}
    {{ log("Checking model name mismatches against SVs listed in " ~ database ~ "." ~ schema ~ "." ~ table_name ~ "." ~ column_name, info=True) }}
    {% if direction != 'both' %}
      {{ log("Direction: " ~ direction, info=True) }}
    {% endif %}

    {# Get source values #}
    {% set source_results = run_query(source_query) %}
    {% set source_names = [] %}
    {% for row in source_results %}
      {% do source_names.append(row[0]) %}
    {% endfor %}

    {# Get model names #}
    {% set model_names = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% if include_disabled or node.config.enabled %}
          {% if not only_this_project or node.package_name == project_name %}
            {% set nm = node.name if case_sensitive else node.name | lower %}
            {% do model_names.append(nm) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {# Find mismatches and log them #}
    {% set source_not_in_models = [] %}
    {% set models_not_in_source = [] %}
    
    {% if direction in ('source', 'both') %}
      {% for name in source_names %}
        {% if name not in model_names %}
          {% do source_not_in_models.append(name) %}
        {% endif %}
      {% endfor %}
    {% endif %}

    {% if direction in ('model', 'both') %}
      {% for name in model_names %}
        {% if name not in source_names %}
          {% do models_not_in_source.append(name) %}
        {% endif %}
      {% endfor %}
    {% endif %}

    {% set total_mismatches = source_not_in_models | length + models_not_in_source | length %}
    
    {% if total_mismatches > 0 %}
      {{ log("Found " ~ total_mismatches ~ " mismatches:", info=True) }}
      {% if source_not_in_models | length > 0 %}
        {{ log("  SVs listed in mapping table but not found in models:", info=True) }}
        {% for name in source_not_in_models %}
          {{ log("    - " ~ name, info=True) }}
        {% endfor %}
      {% endif %}
      {% if models_not_in_source | length > 0 %}
        {{ log("  Models not listed in mapping table:", info=True) }}
        {% for name in models_not_in_source %}
          {{ log("    - " ~ name, info=True) }}
        {% endfor %}
      {% endif %}
    {% else %}
      {{ log("No mismatches found.", info=True) }}
    {% endif %}
  {% endif %}

  {# Build the SQL query #}
  with source_vals as (
    {{ source_query }}
  ),
  model_vals as (
    {% set model_list = [] %}
    {% if execute %}
      {% for nm in model_names %}
        {% do model_list.append("('" ~ (nm | replace("'", "''")) ~ "')") %}
      {% endfor %}
    {% endif %}
    {% if model_list | length > 0 %}
      select column1 as name from values {{ model_list | join(',') }}
    {% else %}
      select null::varchar as name where false
    {% endif %}
  ),
  all_mismatches as (
    {% if direction in ('source', 'both') %}
    select 'in_db_not_in_models' as mismatch_type, s.name as name
    from source_vals s
    left join model_vals m on s.name = m.name
    where m.name is null
    {% endif %}
    {% if direction in ('model', 'both') %}
    {% if direction == 'both' %}union all{% endif %}
    select 'in_models_not_in_db' as mismatch_type, m.name as name
    from model_vals m
    left join source_vals s on s.name = m.name
    where s.name is null
    {% endif %}
  )
  select *
  from all_mismatches
  where 1=1
{% endmacro %}