{{ config(
    schema = 'staking_solana',
    tags = ['dunesql'],
    alias = alias('stake_actions'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_stake_action_id'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

with    
    merge as (
        SELECT 
            abs(aa.balance_change/pow(10,9)) as stake
            , 'merge' as action
            , account_sourceStakeAccount as source
            , account_destinationStakeAccount as destination
            , account_stakeAuthority as authority
            , call_block_slot
            , call_block_time
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_tx_id
        FROM {{ source('stake_program_solana', 'stake_call_Merge') }} m
        LEFT JOIN {{ source('solana', 'account_activity') }} aa ON 1=1 
            AND aa.address = m.account_sourceStakeAccount --the source table gets completely merged so this is safest to join on
            AND aa.block_slot = m.call_block_slot
            AND aa.tx_id = m.call_tx_id
            and aa.writable = true
            and aa.balance_change != 0
            and aa.tx_success
        where 1=1 
        {% if is_incremental() %}
        and m.call_block_time >= date_trunc('day', now() - interval '7' day)
        and aa.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
    
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
        where 1=1 
        {% if is_incremental() %}
        and call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
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
        where 1=1 
        {% if is_incremental() %}
        and call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )

SELECT
    stake
    , action
    , source
    , destination
    , authority
    , call_block_slot as block_slot
    , call_block_time as block_time
    , call_outer_instruction_index as outer_instruction_index
    , call_inner_instruction_index as inner_instruction_index
    , call_tx_id as tx_id
    , concat(call_tx_id,'-',source,'-',destination,'-',cast(stake as varchar),'-',authority,'-',cast(call_outer_instruction_index as varchar)) as unique_stake_action_id
FROM (
    SELECT * FROM merge
    UNION ALL
    SELECT * FROM withdraw
    UNION ALL
    SELECT * FROM split
)
where 1=1 
{% if is_incremental() %}
and call_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}