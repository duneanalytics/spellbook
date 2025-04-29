{{ config(
    schema = 'tokens_solana',
    alias = 'base_sol_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['block_date', 'unique_instruction_key']
) }}

WITH transfers AS (
    select
        cast(date_trunc('day', call_block_time) AS date) AS block_date
        , call_block_time as block_time
        , call_block_slot as block_slot
        , cast(null as varchar) as action
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_instruction_index as outer_instruction_index
        , call_tx_signer as tx_signer
        , lamports as amount
        , call_outer_executing_account as outer_executing_account
        , call_inner_executing_account as inner_executing_account
        , substring(call_account_arguments[1], 1, 2) as from_token_account_prefix
        , call_account_arguments[1] as from_token_account
        , substring(call_account_arguments[2], 1, 2) as to_token_account_prefix
        , call_account_arguments[2] as to_token_account
        , 'native' as token_version
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
        {% if is_incremental() -%}
        and {{incremental_predicate('call_block_time')}}
        {% endif -%}
)
, final AS (
    select
        transfers.*
    from
        transfers
    {% if is_incremental() -%}
    left join
        {{ this }} as existing
        -- typically only inner_instruction_index is null, but coalesce all to be safe
        -- since we're building unique_instruction_key in this model, use individual fields for lookup
        on coalesce(existing.block_date, date '9999-12-31') = coalesce(transfers.block_date, date '9999-12-31')
        and coalesce(existing.block_slot, 0) = coalesce(transfers.block_slot, 0)
        and coalesce(existing.tx_index, 0) = coalesce(transfers.tx_index, 0)
        and coalesce(existing.inner_instruction_index, 0) = coalesce(transfers.inner_instruction_index, 0)
        and coalesce(existing.outer_instruction_index, 0) = coalesce(transfers.outer_instruction_index, 0)
        and {{incremental_predicate('existing.block_time')}}
    where
        existing.block_date is null -- only insert new rows
    {% endif -%}
)
select
    *
from
    final