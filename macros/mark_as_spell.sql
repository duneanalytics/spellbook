{% macro mark_as_spell(this, materialization) %}
{%- if target.name == 'prod'-%}
        ALTER {{"view" if materialization == "view" else "table"}} {{ this }}
        SET TBLPROPERTIES (
        'dune.data_explorer.category'='abstraction'
        )
{%- else -%}
{%- endif -%}
{%- endmacro -%}
