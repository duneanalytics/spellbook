{% macro trino__create_table_as(temporary, relation, sql) -%}
  {%- set _properties = {} -%}
  {%- if config.get('partition_by', None) != None -%}
    {%- do _properties.update({'partitioned_by': "ARRAY['" + (config.get('partition_by') | join("', '") )  + "']"}) -%}
  {%- endif -%}
  create or replace table {{ relation }}
    {{ create_table_properties(_properties, relation) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{# Override dbt-trino's create_view_as to mark CI views as public via extra_properties. #}
{% macro trino__create_view_as(relation, sql) -%}
  {%- set view_security = config.get('view_security', 'definer') -%}
  {%- if view_security not in ['definer', 'invoker'] -%}
      {%- set log_message = 'Invalid value for view_security (%s) specified. Setting default value (%s).' % (view_security, 'definer') -%}
      {% do log(log_message) %}
      {%- set view_security = 'definer' -%}
  {% endif %}
  create or replace view
    {{ relation }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  security {{ view_security }}
  {%- if target.name == 'ci' %}
  with (extra_properties = map_from_entries(ARRAY[ROW('dune.public', 'true')]))
  {%- endif %}
  as
    {{ sql }}
  ;
{% endmacro %}

{# temp fix to get latest dbt-trino version 1.8.3 working in dbt cloud #}
{% macro dune_properties(properties) %}
  {%- if properties is not none and properties | length > 0 -%}
      WITH (
          {%- for key, value in properties.items() -%}
            {{ key }} = {{ value }}
            {%- if not loop.last -%}{{ ',\n  ' }}{%- endif -%}
          {%- endfor -%}
      )
  {%- endif -%}
{%- endmacro -%}

{% macro create_table_properties(_properties, relation) %}
  {%- if not (target.name == 'ci' and target.database == 'dune') -%}
    {%- set modified_identifier = relation.identifier | replace("__dbt_tmp", "") -%}
    {%- set unique_location = modified_identifier ~ '_' ~ time_salted_md5_prefix() -%}
    {%- set location= 's3a://%s/%s/%s' % (s3_bucket(), relation.schema, unique_location) -%}
    {%- do _properties.update({'location': "'" + location + "'"}) -%}
  {%- endif -%}
  {%- if target.name == 'ci' -%}
    {%- do _properties.update({'extra_properties': "map_from_entries(ARRAY[ROW('dune.public', 'true')])"}) -%}
  {%- endif -%}
    {# temp fix to get latest dbt-trino version 1.8.3 working in dbt cloud #}
    {{ dune_properties(_properties) }} {# properties is a macro within the trino adapter #}
{%- endmacro -%}

{% macro time_salted_md5_prefix(input_string=None) -%}
  {% if input_string is none -%}
    {% set input_string = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') %}
  {%- endif %}
  {{- local_md5(input_string) -}}
{%- endmacro %}

{%- macro s3_bucket() -%}
  {%- if target.type == 'trino' and target.schema != 'wizard' -%}
    {%- if target.name == 'prod' or target.schema.startswith('git_dunesql') -%}
      {{- return('prod-spellbook-trino-118330671040') -}}
    {%- else -%}
      {{- return('trino-dev-datasets-118330671040') }}
    {%- endif -%}
  {%- else -%}
    {{- return(var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local')) -}}
  {%- endif -%}
{%- endmacro -%}