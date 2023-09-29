{{ config(
    schema = 'tokens_solana_nft',
    tags = ['dunesql'],
    alias = alias('stake_actions'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_id', 'action','outer_instruction_index'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

with    
    delegate_and_merge as (
        SELECT 
            abs(aa.balance_change/pow(10,9)) as stake
            , all.*
        FROM (
            SELECT
                'delegate' as action
                , account_configAccount as source
                , account_stakeAccount as destination
                , account_stakeAuthority as authority
                , call_block_slot
                , call_block_time
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_tx_id
            FROM {{ source('stake_program_solana', 'stake_call_DelegateStake') }}
            
            UNION ALL 
            
            SELECT
                'merge' as action
                , account_sourceStakeAccount as source
                , account_destinationStakeAccount as destination
                , account_stakeAuthority as authority
                , call_block_slot
                , call_block_time
                , call_outer_instruction_index
                , call_inner_instruction_index
                , call_tx_id
            FROM {{ source('stake_program_solana', 'stake_call_Merge') }}
        ) all
        LEFT JOIN {{ source('solana', 'account_activity') }} aa ON 1=1 
            AND aa.address = all.source
            AND aa.block_slot = all.call_block_slot
            AND aa.tx_id = all.call_tx_id
            and aa.writable = true
            and aa.balance_change != 0
            and aa.tx_success
    )
    
    -- --NOT ACTIVATED YET BY FOUNDATION--
    -- , redelegate as (
    --     SELECT
    --     *
    --     FROM stake_program_solana.stake_call_Redelegate
    --     limit 10
    -- )
    
    , withdraw as (
        SELECT
            lamports/pow(10,9) as stake
            , 'withdraw' as action
            , account_stakeAccount as source
            , account_recipientAccount as destination
            , account_withdrawAuthority as authority
            , call_block_slot
            , call_block_time
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_tx_id
        FROM {{ source('stake_program_solana', 'stake_call_Withdraw') }}
    )
    
    , split as (
        SELECT 
            lamports/pow(10,9) as stake
            , 'split' as action
            , account_stakeAccount as source
            , account_splitStakeAccount as destination
            , account_stakeAuthority as authority
            , call_block_slot
            , call_block_time
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_tx_id
        FROM {{ source('stake_program_solana', 'stake_call_Split') }}
    )

SELECT
    *
FROM (
    SELECT * FROM delegate_and_merge
    UNION ALL
    SELECT * FROM withdraw
    UNION ALL
    SELECT * FROM split
)
{% if is_incremental() %}
WHERE block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}