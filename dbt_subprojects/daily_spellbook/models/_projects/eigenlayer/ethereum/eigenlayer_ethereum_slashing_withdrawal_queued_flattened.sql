{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'slashing_withdrawal_queued_flattened',
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
            {{ source('eigenlayer_ethereum', 'DelegationManager_evt_SlashingWithdrawalQueued') }}
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
            JSON_EXTRACT(parsed_withdrawal, '$.scaledShares') AS ARRAY(VARCHAR)
        )
    )
WITH
    ORDINALITY AS v (shares, ordinality)
WHERE
    u.ordinality = v.ordinality 
