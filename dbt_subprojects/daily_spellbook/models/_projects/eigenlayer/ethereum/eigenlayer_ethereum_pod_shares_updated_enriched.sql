{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'withdrawal_queued_v2_flattened',
        materialized = 'view'
    )
}}


SELECT
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0 AS strategy,
    CAST(sharesDelta AS DECIMAL(38,0)) AS shares
FROM source('eigenlayer_ethereum', 'EigenPodManager_evt_PodSharesUpdated')
