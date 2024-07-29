{{ config(
    schema = 'staking_solana'
    , alias = 'stake_actions'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_time', 'tx_id', 'source', 'destination', 'stake', 'authority', 'outer_instruction_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

with
    aa as (
        SELECT
            address
            , block_slot
            , tx_id
            , balance_change
        FROM
            {{ source('solana', 'account_activity') }}
        WHERE 
            writable = true
            and balance_change != 0
            and token_mint_address is null
            and tx_success
            and block_time >= TIMESTAMP '2020-11-14' --min(block_time) from stake_program_solana.stake_call_Merge, inner joined downstream
            {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
            {% endif %}
    )

    , merge as (
        SELECT 
            abs(aa.balance_change/pow(10,9)) as stake
            , 'merge' as action
            , m.account_sourceStakeAccount as source
            , m.account_destinationStakeAccount as destination
            , m.account_stakeAuthority as authority
            , m.call_block_slot
            , m.call_block_time
            , m.call_outer_instruction_index
            , m.call_inner_instruction_index
            , m.call_tx_id
        FROM aa
        JOIN {{ source('stake_program_solana', 'stake_call_Merge') }} m ON 1=1 
            AND aa.address = m.account_sourceStakeAccount --the source table gets completely merged so this is safest to join on
            AND aa.block_slot = m.call_block_slot
            AND aa.tx_id = m.call_tx_id
        where 1=1 
        {% if is_incremental() %}
        and {{ incremental_predicate('m.call_block_time') }}
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
        and {{ incremental_predicate('call_block_time') }}
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
        and {{ incremental_predicate('call_block_time') }}
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
FROM (
    SELECT * FROM merge
    UNION ALL
    SELECT * FROM withdraw
    UNION ALL
    SELECT * FROM split
)
where 1=1
