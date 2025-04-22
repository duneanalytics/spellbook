{% macro solana_sol_transfers_macro(start_date, end_date) %}

WITH transfers AS (
    select
        call_block_time as block_time
        , cast(date_trunc('day', call_block_time) AS date) AS block_date
        , cast(date_trunc('month', call_block_time) AS date) AS block_month
        , call_block_slot as block_slot
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_instruction_index as outer_instruction_index
        , call_tx_signer as tx_signer
        , lamports as amount
        , lamports / 1e9 as amount_display
        , call_outer_executing_account as outer_executing_account
        , call_inner_executing_account as inner_executing_account
        , substring(call_account_arguments[1], 1, 2) as from_token_account_prefix
        , call_account_arguments[1] as from_token_account
        , substring(call_account_arguments[2], 1, 2) as to_token_account_prefix
        , call_account_arguments[2] as to_token_account
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key --block time is not granular enough, build unique key from block_slot, tx_index, outer_instruction_index, inner_instruction_index
    from
        {{ source('system_program_solana', 'system_program_call_Transfer') }} as t
    where
        1=1
        {% if is_incremental() or true -%}
        and {{incremental_predicate('call_block_time')}}
        {% else -%}
        and call_block_time >= {{start_date}}
        and call_block_time < {{end_date}}
        {% endif -%}
)
, prices AS (
    select
        contract_address,
        minute,
        price,
        decimals
    from
        {{ source('prices', 'usd_forward_fill') }}
    where
        blockchain = 'solana'
        and contract_address =  0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 -- SOL address
        and minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() or true -%}
        and {{incremental_predicate('minute')}}
        {% else -%}
        and minute >= {{start_date}}
        and minute < {{end_date}}
        {% endif -%}
)
select
    'solana' as blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_slot
    , t.tx_id
    , t.tx_index
    , t.inner_instruction_index
    , coalesce(t.inner_instruction_index, 0) as key_inner_instruction_index
    , t.outer_instruction_index
    , t.tx_signer
    , COALESCE(tk_from.token_balance_owner, call_account_arguments[1]) AS from_owner -- if the token account exists, use the owner of that, otherwise it should be an account
    , COALESCE(tk_to.token_balance_owner, call_account_arguments[2]) AS to_owner
    , tk_from.address as from_token_account, -- if the token account exists, use the address of that, otherwise no token accounts are involved
    , tk_to.address as to_token_account
    , 'So11111111111111111111111111111111111111112' as token_mint_address
    , 'SOL' as symbol
    , t.amount_display
    , t.amount
    , p.price * (t.amount_display) as amount_usd
    , p.price as price_usd
    , CASE WHEN tk_to.address IS NOT NULL THEN 'wrap' ELSE 'transfer' END as action -- if the token account exists, it's a wrap, otherwise it's a transfer
    , t.outer_executing_account
    , t.inner_executing_account
    , 'native' as token_version
from
    transfers as t
left join 
    {{ ref('solana_utils_token_accounts_state_history') }} as tk_from    
    on t.from_token_account_prefix = tk_from.address_prefix
    and t.from_token_account = tk_from.address
    and t.unique_instruction_key >= tk_from.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_from.valid_to_unique_instruction_key
left join 
    {{ ref('solana_utils_token_accounts_state_history') }} as tk_to 
    on t.to_token_account_prefix = tk_to.address_prefix
    and t.to_token_account = tk_to.address
    and t.unique_instruction_key >= tk_to.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_to.valid_to_unique_instruction_key
left join
    prices as p
    on p.minute = date_trunc('minute', t.block_time)
{% endmacro %}