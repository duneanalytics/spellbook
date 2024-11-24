{{
    config(
        schema = 'lifi',
        alias = 'transfers',
        materialized = 'incremental',
        unique_key = ['evt_tx_hash', 'evt_index', 'source_chain']
    )
}}

{% set chains = [
    'ethereum',
    'arbitrum',
    'avalanche',
    'bnb',
    'fantom',
    'gnosis',
    'zksync'
] %}

with chain_transfers as (
    {% for chain in chains %}
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        transactionId,
        bridge,
        integrator,
        referrer,
        sendingAssetId,
        receiver,
        minAmount,
        destinationChainId,
        source_chain,
        sender
    from {{ ref('lifi_' ~ chain ~ '_transfers') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    transactionId,
    bridge,
    integrator,
    referrer,
    sendingAssetId,
    receiver,
    minAmount,
    destinationChainId,
    source_chain,
    sender
from chain_transfers