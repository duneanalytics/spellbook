{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'slashing_withdrawal_completed_enriched',
        materialized = 'table'
    )
}}

SELECT
    a.evt_tx_hash,
    a.evt_index,
    a.evt_block_time,
    a.evt_block_number,
    a.withdrawalRoot,
    b.strategy,
    b.shares
FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_SlashingWithdrawalCompleted') }} AS a
JOIN {{ ref('eigenlayer_ethereum_slashing_withdrawal_queued_flattened') }} AS b
    ON a.withdrawalRoot = b.withdrawalRoot 
