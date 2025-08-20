{{ 
    config(
        schema = 'hivemapper_solana',
        alias = 'rewards',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook = '{{ expose_spells(\'["solana"]\',
                                "project",
                                "hivemapper",
                                \'["ilemi", "alexus98"]\') }}')
}}

with 
    honey_transfers as (
        SELECT 
            block_time
            , block_date
            , block_slot
            , "action"
            , amount
            , token_mint_address
            , from_owner
            , to_owner
            , from_token_account
            , to_token_account
            , tx_signer
            , tx_id
            , outer_instruction_index
            , inner_instruction_index
            , outer_executing_account
        
        FROM {{ source('tokens_solana','transfers') }} WHERE token_mint_address = '4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy'

        {% if is_incremental() %}
        and 
            {{ incremental_predicate('block_time') }}
        {% endif %}
    )
    
    , memo_join as (
        SELECT
            case 
                when contains(tx.log_messages,'Program log: Memo (len 12): "Map Coverage"') then 'map coverage'
                when contains(tx.log_messages,'Program log: Memo (len 6): "Bounty"') then 'bounty'
                when contains(tx.log_messages,'Program log: Memo (len 4): "Buzz"') then 'buzz'
                when contains(tx.log_messages,'Program log: Memo (len 18): "Map Editing and QA"') then 'QA (AI trainer)'
                when contains(tx.log_messages,'Program log: Memo (len 15): "Map Consumption"') then 'map consumption'
                when contains(tx.log_messages,'Program log: Memo (len 23): "Map Consumption (fleet)"') then 'map consumption (fleet)'
                when contains(tx.log_messages,'Program log: Memo (len 17): "Foundation Reward"') then 'FTM'
                when contains(tx.log_messages,'Program log: Memo (len 11): "Honey Burst"') then 'burst'
                else null
            end as reward_type
            , hny.*
        FROM {{ source('solana', 'transactions') }} tx
        JOIN honey_transfers hny ON tx.id = hny.tx_id --assumes only one transfer per tx
        WHERE 1=1 
        and contains(account_keys, 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') --memo program invoked sometimes not by honey though
        {% if is_incremental() %}
        and 
            {{ incremental_predicate('tx.block_time') }}
        {% endif %}

    ) 
    
SELECT
*
FROM memo_join
WHERE reward_type is not null

