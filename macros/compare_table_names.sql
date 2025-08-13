{% macro compare_table_names_to_models(database, schema, table_name, column_name, only_this_project=True, include_disabled=False, case_sensitive=False) %}
--call with "dbt run-operation compare_table_names_to_models --args '{database: "FRISBIE_DEVELOPMENT", schema: "SANDBOX", table_name: "SNOW_SV_LIST", column_name: "SNOW_SV_NAME", case_sensitive: false}'"

  {# Resolve the relation in the target warehouse #}
  {% set relation = adapter.get_relation(database=database, schema=schema, identifier=table_name) %}
  {% if relation is none %}
    {{ exceptions.raise_compiler_error("Relation " ~ database ~ "." ~ schema ~ "." ~ table_name ~ " not found.") }}
  {% endif %}

  {% set quoted_column = adapter.quote(column_name) %}
  {% set sql %}
    select distinct {{ quoted_column }} as table_name
    from {{ relation }}
  {% endset %}

  {% set results = run_query(sql) %}
  {% if results is none %}
    {{ log("No results returned. Are you running this with `dbt run-operation` or inside an on-run-* hook?", info=True) }}
    {{ return(tojson({
      "status": "no_results",
      "database": database,
      "schema": schema,
      "table_name": table_name,
      "column_name": column_name
    })) }}
  {% endif %}

  {# Collect table names from the source table #}
  {% set source_table_names = [] %}
  {% for row in results.rows %}
    {% if row[0] is not none %}
      {% if case_sensitive %}
        {% set value = row[0] | string %}
      {% else %}
        {% set value = (row[0] | string) | lower %}
      {% endif %}
      {% do source_table_names.append(value) %}
    {% endif %}
  {% endfor %}

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

  {% set source_unique = source_table_names | unique | list %}
  {% set model_unique = model_name_set | unique | list %}

  {% set in_source_not_in_models = [] %}
  {% for name in source_unique %}
    {% if name not in model_unique %}
      {% do in_source_not_in_models.append(name) %}
    {% endif %}
  {% endfor %}

  {% set in_models_not_in_source = [] %}
  {% for name in model_unique %}
    {% if name not in source_unique %}
      {% do in_models_not_in_source.append(name) %}
    {% endif %}
  {% endfor %}

  {{ log("Count in source: " ~ (source_unique | length) ~ ", count in models: " ~ (model_unique | length), info=True) }}
  {{ log("In source not in models: " ~ (in_source_not_in_models | length), info=True) }}
  {{ log(in_source_not_in_models | join(", "), info=True) }}
  {{ log("In models not in source: " ~ (in_models_not_in_source | length), info=True) }}
  {{ log(in_models_not_in_source | join(", "), info=True) }}

  {% set payload = {
    "database": database,
    "schema": schema,
    "table_name": table_name,
    "column_name": column_name,
    "case_sensitive": case_sensitive,
    "only_this_project": only_this_project,
    "include_disabled": include_disabled,
    "counts": {
      "source": source_unique | length,
      "models": model_unique | length,
      "in_source_not_in_models": in_source_not_in_models | length,
      "in_models_not_in_source": in_models_not_in_source | length
    },
    "in_source_not_in_models": in_source_not_in_models,
    "in_models_not_in_source": in_models_not_in_source
  } %}

  {{ return(tojson(payload)) }}

{% endmacro %}


