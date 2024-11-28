{{
    config(
        schema = 'lifi',
        alias = 'transfers',
        materialized = 'view',
        contributor = 'lequangphu'
    )
}}

{% set chains = [
    'ethereum',
    'arbitrum',
    'avalanche_c',
    'bnb',
    'fantom',
    'gnosis',
    'zksync'
] %}

with chain_transfers as (
    {% for chain in chains %}
    select 
        contract_address,
        tx_hash,
        evt_index,
        block_time,
        block_number,
        block_date,
        transactionId,
        bridge,
        integrator,
        referrer,
        sendingAssetId,
        receiver,
        minAmount,
        destinationChainId,
        source_chain,
        transfer_id,
        amount_usd,
        tx_from
    from {{ ref('lifi_' ~ chain ~ '_transfers') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from chain_transfers