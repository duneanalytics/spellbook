{{ config(
    schema = 'staking_solana',
    alias = 'staking_data',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'event_index'],
    tags = ['solana', 'staking']
) }}

WITH delegate_stake_data AS (
    SELECT
        cc.call_block_time AS block_time,
        ci.call_block_slot AS block_slot,
        ci.call_tx_id AS tx_id,
        dd.account_stakeAccount AS stake_account,
        dd.account_stakeAuthority AS stake_authority,
        SPLIT_PART(SPLIT_PART(JSON_EXTRACT_SCALAR(ci.authorized, '$.Authorized.withdrawer'), '(', 2), ')', 1) AS withdraw_authority,
        dd.account_voteAccount AS vote_account,
        SPLIT_PART(SPLIT_PART(JSON_EXTRACT_SCALAR(ci.authorized, '$.Authorized.staker'), '(', 2), ')', 1) AS staker_pubkey,
        cc.lamports AS delegate_lamports,
        'Delegate' AS action_type,
        ROW_NUMBER() OVER (PARTITION BY ci.call_tx_id ORDER BY ci.call_inner_instruction_index) AS event_index
    FROM {{ source('stake_program_solana', 'stake_call_initialize') }} ci
    INNER JOIN {{ source('stake_program_solana', 'stake_call_delegatestake') }} dd
        ON dd.call_tx_id = ci.call_tx_id
    INNER JOIN {{ source('system_program_solana', 'system_program_call_createaccount') }} cc
        ON ci.call_tx_id = cc.call_tx_id
        AND cc.account_newAccount = dd.account_stakeAccount
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cc.call_block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        cc.call_block_time AS block_time,
        ci.call_block_slot AS block_slot,
        ci.call_tx_id AS tx_id,
        dd.account_stakeAccount AS stake_account,
        dd.account_stakeAuthority AS stake_authority,
        SPLIT_PART(SPLIT_PART(JSON_EXTRACT_SCALAR(ci.authorized, '$.Authorized.withdrawer'), '(', 2), ')', 1) AS withdraw_authority,
        dd.account_voteAccount AS vote_account,
        SPLIT_PART(SPLIT_PART(JSON_EXTRACT_SCALAR(ci.authorized, '$.Authorized.staker'), '(', 2), ')', 1) AS staker_pubkey,
        cc.lamports AS delegate_lamports,
        'Delegate' AS action_type,
        ROW_NUMBER() OVER (PARTITION BY ci.call_tx_id ORDER BY ci.call_inner_instruction_index) AS event_index
    FROM {{ source('stake_program_solana', 'stake_call_initializechecked') }} ci
    INNER JOIN {{ source('stake_program_solana', 'stake_call_delegatestake') }} dd
        ON dd.call_tx_id = ci.call_tx_id
    INNER JOIN {{ source('system_program_solana', 'system_program_call_createaccount') }} cc
        ON ci.call_tx_id = cc.call_tx_id
        AND cc.account_newAccount = dd.account_stakeAccount
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cc.call_block_time') }}
    {% endif %}
),
split_stake_data AS (
    SELECT
        scs.call_block_time AS block_time,
        scs.call_block_slot AS block_slot,
        scs.call_tx_id AS tx_id,
        scs.account_stakeAccount AS stake_account,
        scs.account_splitStakeAccount AS split_stake_account,
        scs.account_stakeAuthority AS stake_authority,
        scs.lamports AS split_lamports,
        'Split' AS action_type,
        ROW_NUMBER() OVER (PARTITION BY scs.call_tx_id ORDER BY scs.call_inner_instruction_index) AS event_index
    FROM {{ source('stake_program_solana', 'stake_call_split') }} scs
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('scs.call_block_time') }}
    {% endif %}
),
deactivate_stake_data AS (
    SELECT
        scd.call_block_time AS block_time,
        scd.call_block_slot AS block_slot,
        scd.call_tx_id AS tx_id,
        scd.account_delegatedStakeAccount AS stake_account,
        scd.account_stakeAuthority AS stake_authority,
        'Deactivate' AS action_type,
        ROW_NUMBER() OVER (PARTITION BY scd.call_tx_id ORDER BY scd.call_inner_instruction_index) AS event_index
    FROM {{ source('stake_program_solana', 'stake_call_deactivate') }} scd
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('scd.call_block_time') }}
    {% endif %}
),
all_staking_events AS (
    SELECT
        block_time,
        block_slot,
        tx_id,
        event_index,
        stake_account,
        stake_authority,
        withdraw_authority,
        vote_account,
        delegate_lamports AS lamports,
        NULL AS split_stake_account,
        NULL AS split_lamports,
        action_type
    FROM delegate_stake_data
    UNION ALL
    SELECT
        block_time,
        block_slot,
        tx_id,
        event_index,
        stake_account,
        stake_authority,
        NULL AS withdraw_authority,
        NULL AS vote_account,
        -split_lamports AS lamports, -- Subtract from original stake account
        split_stake_account,
        split_lamports,
        action_type
    FROM split_stake_data
    UNION ALL
    SELECT
        block_time,
        block_slot,
        tx_id,
        event_index,
        split_stake_account AS stake_account, -- Treat split account as a new stake account
        stake_authority,
        NULL AS withdraw_authority,
        NULL AS vote_account,
        split_lamports AS lamports, -- Add to new split account
        NULL AS split_stake_account,
        NULL AS split_lamports,
        'Delegate' AS action_type -- Treat as a new delegation
    FROM split_stake_data
    UNION ALL
    SELECT
        block_time,
        block_slot,
        tx_id,
        event_index,
        stake_account,
        stake_authority,
        NULL AS withdraw_authority,
        NULL AS vote_account,
        NULL AS lamports,
        NULL AS split_stake_account,
        NULL AS split_lamports,
        action_type
    FROM deactivate_stake_data
),
stake_account_state AS (
    SELECT
        stake_account,
        block_time,
        block_slot,
        tx_id,
        event_index,
        action_type,
        stake_authority,
        withdraw_authority,
        vote_account,
        lamports,
        split_stake_account,
        SUM(lamports) OVER (
            PARTITION BY stake_account
            ORDER BY block_time, block_slot, event_index
        ) AS current_lamports,
        CASE
            WHEN MAX(CASE WHEN action_type = 'Deactivate' THEN 1 ELSE 0 END) OVER (
                PARTITION BY stake_account
                ORDER BY block_time, block_slot, event_index
            ) = 1 THEN FALSE
            ELSE TRUE
        END AS is_active
    FROM all_staking_events
    WHERE stake_account IS NOT NULL
)
SELECT
    stake_account,
    block_time,
    block_slot,
    tx_id,
    event_index,
    action_type,
    stake_authority,
    withdraw_authority,
    vote_account,
    current_lamports,
    current_lamports / 1e9 AS current_sol,
    split_stake_account,
    is_active
FROM stake_account_state
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
ORDER BY block_time, block_slot, event_index;
