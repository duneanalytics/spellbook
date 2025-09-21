{{
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'table',
        unique_key = ['blockchain'],
    )
}}

{% set meta = oneinch_meta_cfg_macro(property = 'blockchains') %}

select *
from (
    {% for blockchain in meta['start'] %}
        select
            '{{ blockchain }}' as blockchain
            , {{ meta.chain_id.get(blockchain, 'null') }} as chain_id
            , date('{{ meta['start'].get(blockchain, 'null') }}') as first_deployed_at
            , '{{ meta.native_token_symbol.get(blockchain, '') }}' as native_token_symbol
            , {{ meta['wrapped_native_token_address'].get(blockchain, 'null') }} as wrapped_native_token_address
            , '{{ meta['explorer_link'].get(blockchain, 'null') }}' as explorer_link
            , array[{{ meta['fusion_settlement_addresses'].get(blockchain, 'null') | join(', ') }}] as fusion_settlement_addresses
            , array[{{ meta['escrow_factory_addresses'].get(blockchain, 'null') | join(', ') }}] as escrow_factory_addresses
            , '{{ meta['exposed'].get(blockchain, '') }}' as exposed
        {% if not loop.last %}union{% endif %}
    {% endfor %}
)