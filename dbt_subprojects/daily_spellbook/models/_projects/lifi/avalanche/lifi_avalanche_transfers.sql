{{ config(
    schema = 'lifi_avalanche',
    alias = 'transfers',
    materialized = 'view'
    )
}}

with source_data as (
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        json_extract_scalar(bridgeData, '$.transactionId') as transactionId,
        json_extract_scalar(bridgeData, '$.bridge') as bridge,
        json_extract_scalar(bridgeData, '$.integrator') as integrator,
        json_extract_scalar(bridgeData, '$.referrer') as referrer,
        json_extract_scalar(bridgeData, '$.sendingAssetId') as sendingAssetId,
        json_extract_scalar(bridgeData, '$.receiver') as receiver,
        json_extract_scalar(bridgeData, '$.minAmount') as minAmount,
        json_extract_scalar(bridgeData, '$.destinationChainId') as destinationChainId,
        'avalanche' as source_chain
    from {{ source('lifi_avalanche', 'LiFiDiamond_v2_evt_LiFiTransferStarted') }}
),
transactions as (
    select 
        "from" as sender,
        hash as tx_hash
    from {{ source('avalanche_c', 'transactions') }}
)

select 
    s.*,
    t.sender
from source_data s
inner join transactions t
    on s.evt_tx_hash = t.tx_hash
