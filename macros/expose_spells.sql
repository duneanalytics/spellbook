{% macro expose_spells(blockchains, spell_type, spell_name, contributors) %}
{%- if target.name == 'prod'-%}
        ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
        SET TBLPROPERTIES (
        'dune.public'='true',
        'dune.data_explorer.blockchains'= '{{ blockchains }}',     -- e.g., ["ethereum","solana"]
        'dune.data_explorer.category'='abstraction',
        'dune.data_explorer.abstraction.type'= '{{ spell_type }}', -- 'project' or 'sector'
        'dune.data_explorer.abstraction.name'= '{{ spell_name }}', -- 'aave' or 'uniswap'
        'dune.data_explorer.contributors'= '{{ contributors }}'   -- e.g., ["soispoke","jeff_dude"]
        )
{%- else -%}
{%- endif -%}
{%- endmacro -%}
