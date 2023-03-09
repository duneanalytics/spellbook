{% macro mark_as_spell() %}
{%- if target.name == 'prod'-%}
        ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
        SET TBLPROPERTIES (
        'dune.data_explorer.category'='abstraction',
        )
{%- else -%}
{%- endif -%}
{%- endmacro -%}
