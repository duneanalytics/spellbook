{% macro solana_sol_transfers_macro(start_date, end_date) %}

WITH transfers AS (
    select
        cast(date_trunc('day', call_block_time) AS date) AS block_date
        , call_block_time as block_time
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
        contract_address
        , minute
        , price
        , decimals
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
), transfer_amounts AS (
    select
        'solana' as blockchain
        , t.block_date
        , t.block_time
        , t.block_slot
        , t.tx_id
        , t.tx_index
        , t.inner_instruction_index
        , t.outer_instruction_index
        , t.unique_instruction_key
        , t.outer_executing_account
        , t.inner_executing_account
        , t.from_token_account_prefix
        , t.from_token_account
        , t.to_token_account_prefix
        , t.to_token_account
        , t.tx_signer
        , 'So11111111111111111111111111111111111111112' as token_mint_address
        , 'SOL' as symbol
        , 'native' as token_version
        , t.amount_display
        , t.amount
        , p.price as price_usd
        , p.price * (t.amount_display) as amount_usd
    from
        transfers as t
    left join
        prices as p
        on p.minute = date_trunc('minute', t.block_time)
)
, final AS (
    select
        transfer_amounts.*
    from
        transfer_amounts
    {% if is_incremental() -%}
    left join
        {{ this }} as existing
        on existing.block_date = transfer_amounts.block_date
        and existing.block_slot = transfer_amounts.block_slot
        and existing.tx_index = transfer_amounts.tx_index
        and existing.inner_instruction_index = transfer_amounts.inner_instruction_index
        and existing.outer_instruction_index = transfer_amounts.outer_instruction_index
        and {{incremental_predicate('existing.block_time')}}
    where
        existing.block_date is null -- only insert new rows
    {% endif -%}
)
select
    *
from
    final
{% endmacro %}