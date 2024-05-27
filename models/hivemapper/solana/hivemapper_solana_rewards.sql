{{ 
    config(
        schema = 'hivemapper_solana',
        alias = 'rewards',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot'],
        post_hook = '{{ expose_spells(\'["solana"]\',
                                "project",
                                "hivemapper",
                                \'["ilemi", "alexus98"]\') }}')
}}

with 
    honey_transfers as (
        SELECT * FROM {{ ref('tokens_solana_transfers') }} WHERE token_mint_address = '4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy'
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
        and tx.block_time >= (select coalesce(max(block_time), timestamp '2022-11-01') from {{ this }})
        {% endif %}

        and tx.block_time >= dateadd(day, -7, current_date) -- delete on merge
    )
    
SELECT
*
FROM memo_join
WHERE reward_type is not null

