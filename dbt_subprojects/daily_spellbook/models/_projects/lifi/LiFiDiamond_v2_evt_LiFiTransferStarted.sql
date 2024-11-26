{{
    config(
        schema = 'lifi',
        alias = 'LiFiDiamond_v2_evt_LiFiTransferStarted',
        materialized = 'view',
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
        tx_from,
        transfer_id
    from {{ ref('lifi_' ~ chain ~ '_transfers') }}

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
    tx_from,
    transfer_id
from chain_transfers