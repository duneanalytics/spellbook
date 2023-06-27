{% macro mark_as_spell(this, materialization) %}
{%- if target.name == 'prod'-%}
        {%- if 'dunesql' not in model.config.get("tags") -%}
                ALTER {{"view" if materialization == "view" else "table"}} {{ this }}
                SET TBLPROPERTIES (
                'dune.data_explorer.category'='abstraction'
                )
        {%- else -%}
                {%- if model.config.materialized != "view" -%} 
                        ALTER {{"view" if materialization == "view" else "table"}} {{ this }}
                        SET PROPERTIES extra_properties = map_from_entries (ARRAY[
                        ROW('dune.data_explorer.category', 'abstraction')
                        ])
                {%- endif -%}
        {%- endif -%}
{%- endif -%}
{%- endmacro -%}
