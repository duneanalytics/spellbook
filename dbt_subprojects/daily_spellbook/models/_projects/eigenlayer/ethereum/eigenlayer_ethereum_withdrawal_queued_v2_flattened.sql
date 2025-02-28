{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'withdrawal_queued_v2_flattened',
        materialized = 'table'
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
    from_hex(substr(u.strategy, 3)) AS strategy,
    CAST(v.shares AS DECIMAL(38,0)) AS shares
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
    ORDINALITY AS v (shares, ordinality)
WHERE
    u.ordinality = v.ordinality
    AND evt_block_number >= 19613848
