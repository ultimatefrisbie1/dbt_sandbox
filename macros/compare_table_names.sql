{% macro get_model_name_mismatches_sql(database, schema, table_name, column_name, only_this_project=True, include_disabled=False, case_sensitive=False, direction='both') %}
--   This macro compares all dbt model names against a list of names in a db table
--   Database, schema, table_name, column_name parameters point to the location of the table and column containing the list to compare against
--   Only_this_project and include_disabled control scope of models compared (defaults True, False)
--   Case sensitive (default False) compares names with oroginal cases if True
--   Direction (default 'both') compares direction of comparison.  Options are:
--      - 'source' - identifies missing models that are listed in the table
--      - 'model' - identifies existing models that are missing in the table
  
  {# Collect dbt model names from the project graph #}
  {% set model_name_set = [] %}
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' %}
      {% if include_disabled or node.config.enabled %}
        {% if not only_this_project or node.package_name == project_name %}
          {% if case_sensitive %}
            {% set nm = node.name %}
          {% else %}
            {% set nm = node.name | lower %}
          {% endif %}
          {% do model_name_set.append(nm) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% set model_unique = model_name_set | unique | list %}

  {# Prepare VALUES list for model names #}
  {% set quoted_models = [] %}
  {% for nm in model_unique %}
    {% set val = "'" ~ (nm | replace("'", "''")) ~ "'" %}
    {% do quoted_models.append(val) %}
  {% endfor %}

  {% set quoted_column = adapter.quote(column_name) %}
  {% if case_sensitive %}
    {% set source_select = 'select distinct ' ~ quoted_column ~ ' as name from ' ~ adapter.quote(database) ~ '.' ~ adapter.quote(schema) ~ '.' ~ adapter.quote(table_name) %}
  {% else %}
    {% set source_select = 'select distinct lower(' ~ quoted_column ~ ') as name from ' ~ adapter.quote(database) ~ '.' ~ adapter.quote(schema) ~ '.' ~ adapter.quote(table_name) %}
  {% endif %}

  {% if quoted_models | length > 0 %}
    {% set model_values_sql = 'select name from (values ' ~ ('(' ~ (quoted_models | join('),(')) ~ ')') ~ ') as m(name)' %}
  {% else %}
    {# produce an empty set #}
    {% set model_values_sql = 'select name from (select null as name) where 1=0' %}
  {% endif %}

  with source_vals as (
    {{ source_select }}
  ),
  model_vals as (
    {{ model_values_sql }}
  ),
  all_mismatches as (
    {% if direction in ('source', 'both') %}
    select 'in_source_not_in_models' as mismatch_type, s.name as name
    from source_vals s
    left join model_vals m on s.name = m.name
    where m.name is null
    {% endif %}
    {% if direction in ('model', 'both') %}
    {% if direction == 'both' %}union all{% endif %}
    select 'in_models_not_in_source' as mismatch_type, m.name as name
    from model_vals m
    left join source_vals s on s.name = m.name
    where s.name is null
    {% endif %}
  )
  select *
  from all_mismatches
  where 1=1  -- ensures valid SQL even if no direction matches
{% endmacro %}