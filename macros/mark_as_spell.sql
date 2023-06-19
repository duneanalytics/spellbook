{% macro mark_as_spell(this, materialization) %}
{%- if target.name == 'prod' or True-%}
        {%- if env_var('DBT_DUNE_SQL', 'False') != 'True' -%}
                ALTER {{"view" if materialization == "view" else "table"}} {{ this }}
                SET TBLPROPERTIES (
                'dune.data_explorer.category'='abstraction'
                )
        {%- else -%}
                ALTER {{"view" if materialization == "view" else "table"}} {{ this }}
                SET PROPERTIES extra_properties = map_from_entries (ARRAY[
                ROW('dune.data_explorer.category', 'abstraction')
                ])
        {%- endif -%}
{%- endif -%}
{%- endmacro -%}
