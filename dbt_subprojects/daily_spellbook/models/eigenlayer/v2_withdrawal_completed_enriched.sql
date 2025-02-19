{{ 
    config(
        schema = 'eigenlayer',
        alias = 'v2_withdrawal_completed_enriched'
    )
}}


SELECT
    a.evt_tx_hash,
    a.evt_index,
    a.evt_block_time,
    a.evt_block_number,
    a.withdrawalRoot,
    b.strategy,
    b.share
FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_WithdrawalCompleted') }} AS a
JOIN {{ ref('eigenlayer_v2_withdrawal_queued_flattened') }} AS b
    ON a.withdrawalRoot = b.withdrawalRoot
