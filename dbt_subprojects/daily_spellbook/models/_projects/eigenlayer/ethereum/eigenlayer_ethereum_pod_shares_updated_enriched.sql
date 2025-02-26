{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'pod_shares_updated_enriched',
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
FROM {{ source('eigenlayer_ethereum', 'EigenPodManager_evt_PodSharesUpdated') }}
