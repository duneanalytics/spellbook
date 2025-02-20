{{ 
    config(
        schema = 'eigenlayer',
        alias = 'withdrawal_completed_v2_enriched',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
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
FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_WithdrawalCompleted') }} AS a
JOIN {{ ref('eigenlayer_withdrawal_queued_v2_flattened') }} AS b
    ON a.withdrawalRoot = b.withdrawalRoot
