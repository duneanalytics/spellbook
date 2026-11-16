{{
    config(
        schema = 'bungee',
        alias = 'bridges',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
    )
}}

{% set chains = [
    'ethereum', 'scroll', 'linea', 'mantle', 'optimism',
    'gnosis', 'arbitrum', 'zksync', 'base', 'bnb', 'polygon',
    'avalanche_c'
] %}

with bungee_bridges as (
    {% for chain in chains %}
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        amount,
        token,
        toChainId,
        bridgeName,
        sender,
        receiver,
        metadata,
        source_chain,
        transfer_id,
        amount_usd
    from {{ ref( 'bungee_' ~ chain ~ '_bridges' ) }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from bungee_bridges