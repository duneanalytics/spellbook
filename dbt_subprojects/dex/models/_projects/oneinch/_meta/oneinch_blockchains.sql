{{-
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'table',
        unique_key = ['blockchain'],
    )
-}}



{%- for blockchain in oneinch_blockchains_cfg_macro() %}
    select
        '{{ blockchain.name }}' as blockchain
        , {{ blockchain.get('chain_id', 'null') }} as chain_id
        , date('{{ blockchain.start }}') as first_deployed_at
        , {{ blockchain.get('native_token_symbol', 'null') }} as native_token_symbol
        , {{ blockchain.get('wrapped_native_token_address', 'cast(null as varbinary)') }} as wrapped_native_token_address
        , {{ blockchain.get('explorer_link', 'null') }} as explorer_link
        , array[{{ blockchain.get('fusion_settlement_addresses', []) | join(', ') }}] as fusion_settlement_addresses
        , array[{{ blockchain.get('escrow_factory_addresses', []) | join(', ') }}] as escrow_factory_addresses
    {% if not loop.last -%} union {%- endif %}
{%- endfor -%}