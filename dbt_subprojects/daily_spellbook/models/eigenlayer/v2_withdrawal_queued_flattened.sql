{{ 
    config(
        schema = 'eigenlayer',
        alias = 'v2_withdrawal_queued_flattened',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


WITH
    parsed_data AS (
        SELECT
            evt_tx_hash,
            evt_index,
            evt_block_time,
            evt_block_number,
            withdrawalRoot,
            JSON_PARSE(withdrawal) AS parsed_withdrawal
        FROM
            {{ source('eigenlayer_ethereum', 'DelegationManager_evt_WithdrawalQueued') }}
    )
SELECT
    t.evt_tx_hash,
    t.evt_index,
    t.evt_block_time,
    t.evt_block_number,
    t.withdrawalRoot,
    u.strategy,
    v.share
FROM
    parsed_data AS t
    CROSS JOIN UNNEST (
        TRY_CAST(
            JSON_EXTRACT(parsed_withdrawal, '$.strategies') AS ARRAY(VARCHAR)
        )
    )
WITH
    ORDINALITY AS u (strategy, ordinality)
    CROSS JOIN UNNEST (
        TRY_CAST(
            JSON_EXTRACT(parsed_withdrawal, '$.shares') AS ARRAY(VARCHAR)
        )
    )
WITH
    ORDINALITY AS v (share, ordinality)
WHERE
    u.ordinality = v.ordinality
    AND evt_block_number >= 19613848
