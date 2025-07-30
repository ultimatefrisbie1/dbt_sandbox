{% materialization no_materialization, default %}
  {%- set target_relation = api.Relation.create(
      identifier=model['alias'],
      schema=model['schema'],
      database=model['database'],
      type='view'
  ) -%}

  -- get the adapter
  {%- set adapter = context['adapter'] -%}

  {%- call statement('main', fetch_result=False, auto_begin=False) -%}
    -- Do nothing
    select 1 limit 0
  {%- endcall -%}

  -- Return empty relations list
  {{ return({'relations': []}) }}

{% endmaterialization %}