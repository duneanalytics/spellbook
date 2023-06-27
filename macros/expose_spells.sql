{% macro expose_spells(blockchains, spell_type, spell_name, contributors) %}
{%- if target.name == 'prod'-%}
        {%- if 'dunesql' not in model.config.get("tags") -%}
                {# comment #}
                ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
                SET TBLPROPERTIES (
                'dune.public'='true',
                'dune.data_explorer.blockchains'= '{{ blockchains }}',     -- e.g., ["ethereum","solana"]
                'dune.data_explorer.category'='abstraction',
                'dune.data_explorer.abstraction.type'= '{{ spell_type }}', -- 'project' or 'sector'
                'dune.data_explorer.abstraction.name'= '{{ spell_name }}', -- 'aave' or 'uniswap'
                'dune.data_explorer.contributors'= '{{ contributors }}',   -- e.g., ["soispoke","jeff_dude"]
                'dune.vacuum' = '{"enabled":true}'
                )
        {%- else -%}
                {%- if model.config.materialized != "view" -%}
                        ALTER {{"view" if model.config.materialized == "view" else "table"}} {{ this }}
                        SET PROPERTIES extra_properties = map_from_entries(ARRAY[
                        ROW('dune.public', 'true'),
                        ROW('dune.data_explorer.blockchains', '{{ blockchains }}'),     -- e.g., ["ethereum","solana"]
                        ROW('dune.data_explorer.category', 'abstraction'),
                        ROW('dune.data_explorer.abstraction.type', '{{ spell_type }}'), -- 'project' or 'sector'
                        ROW('dune.data_explorer.abstraction.name', '{{ spell_name }}'), -- 'aave' or 'uniswap'
                        ROW('dune.data_explorer.contributors','{{ contributors }}')   -- e.g., ["soispoke","jeff_dude"]
                        ])
                {%- endif -%}
        {%- endif -%}
{%- endif -%}
{%- endmacro -%}
