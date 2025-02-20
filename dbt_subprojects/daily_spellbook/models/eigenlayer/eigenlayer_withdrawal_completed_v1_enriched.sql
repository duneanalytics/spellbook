{{ 
    config(
        schema = 'eigenlayer',
        alias = 'withdrawal_completed_v1_enriched',
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
    b.share
FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_WithdrawalCompleted') }} AS a
JOIN {{ source('eigenlayer_ethereum', 'StrategyManager_evt_ShareWithdrawalQueued') }} AS b
    ON a.withdrawalRoot = b.withdrawalRoot
