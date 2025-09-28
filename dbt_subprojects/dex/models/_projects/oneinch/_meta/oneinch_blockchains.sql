{{
    config(
        schema = 'oneinch',
        alias = 'blockchains',
        materialized = 'table',
        unique_key = ['blockchain'],
    )
}}

{% set meta = oneinch_meta_cfg_macro()['blockchains'] %}

select *
from (
    {% for blockchain in meta['start'] %}
        select
            '{{ blockchain }}' as blockchain
            , {{ meta['chain_id'].get(blockchain, 'null') }} as chain_id
            , date('{{ meta['start'][blockchain] }}') as first_deployed_at
            , {{ meta['native_token_symbol'].get(blockchain, 'null') }} as native_token_symbol
            , {{ meta['wrapped_native_token_address'].get(blockchain, 'cast(null as varbinary)') }} as wrapped_native_token_address
            , {{ meta['explorer_link'].get(blockchain, 'null') }} as explorer_link
            , array[{{ meta['fusion_settlement_addresses'].get(blockchain, []) | join(', ') }}] as fusion_settlement_addresses
            , array[{{ meta['escrow_factory_addresses'].get(blockchain, []) | join(', ') }}] as escrow_factory_addresses
            , {{ blockchain in meta['exposed'] }} as exposed
        {% if not loop.last %}union{% endif %}
    {% endfor %}
)