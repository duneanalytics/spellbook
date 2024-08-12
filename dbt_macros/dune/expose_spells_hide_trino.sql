{% macro expose_spells_hide_trino(blockchains, spell_type, spell_name, contributors) %}
{%- if target.name == 'prod'-%}
        ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
        SET TBLPROPERTIES (
        'dune.public'='true',
        'dune.disabled_datasets'='11',
        'dune.data_explorer.blockchains'= '{{ blockchains }}',     -- e.g., ["ethereum","solana"]
        'dune.data_explorer.category'='abstraction',
        'dune.data_explorer.abstraction.type'= '{{ spell_type }}', -- 'project' or 'sector'
        'dune.data_explorer.abstraction.name'= '{{ spell_name }}', -- 'aave' or 'uniswap'
        'dune.data_explorer.contributors'= '{{ contributors }}',   -- e.g., ["soispoke","jeff_dude"]
        'dune.vacuum' = '{"enabled":true}'
        )
{%- else -%}
{%- endif -%}
{%- endmacro -%}
