{%- set exposed = transfers_from_traces_exposed_blockchains_macro() -%}

{{-
    config(
        schema = 'tokens',
        alias = 'tft',
        post_hook = '{{ expose_spells(
            blockchains = \'exposed\', 
            spell_type = "sector",
            spell_name = "tokens",
            contributors = \'["max-morrow", "grkhr"]\'
        ) }}'
    )
-}}

select *
from {{ ref('tokens_transfers_from_traces') }}