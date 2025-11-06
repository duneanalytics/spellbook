{%- set exposed = transfers_from_traces_exposed_blockchains_macro() -%}

{{-
    config(
        schema = 'tokens',
        alias = 'transfers_from_traces',
        post_hook = '{{ expose_spells(
            blockchains = \'exposed\', 
            spell_type = "sector",
            spell_name = "tokens",
            contributors = \'["max-morrow", "grkhr"]\'
        ) }}'
    )
-}}

select
    blockchain
    , block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , trace_address
    , type
    , token_standard
    , contract_address
    , amount_raw
    , "from"
    , "to"
    , unique_key
from ({% for blockchain in exposed %}
    select * from {{ ref('tokens_' + blockchain + '_transfers_from_traces') }}
    {% if not loop.last %}union all{% endif %}
{% endfor %})