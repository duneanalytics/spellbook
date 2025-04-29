{{ config(
    schema = 'tokens_solana',
    alias = 'base_spl_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['block_date', 'unique_instruction_key']
) }}

WITH transfers_raw AS (
    SELECT
        call_block_time as block_time
        , cast(date_trunc('day', call_block_time) as date) as block_date
        , call_block_slot as block_slot
        , 'transfer' as action
        , amount
        , account_source as from_token_account
        , account_destination as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_transferChecked') }}
    WHERE 
        1=1
        {% if is_incremental() -%}
        AND {{incremental_predicate('call_block_time')}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as varchar) as from_token_account
        , account_account as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintTo') }}
    WHERE 
        1=1
        {% if is_incremental() -%}
        AND {{incremental_predicate('call_block_time')}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , call_block_slot as block_slot
        , 'mint' as action
        , amount
        , cast(null as varchar) as from_token_account
        , account_account as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_mintToChecked') }}
    WHERE 
        1=1
        {% if is_incremental() -%}
        AND {{incremental_predicate('call_block_time')}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , account_account as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burn') }}
    WHERE 
        1=1
        {% if is_incremental() -%}
        AND {{incremental_predicate('call_block_time')}}
        {% endif -%}

    UNION ALL

    SELECT
        call_block_time as block_time
        , cast (date_trunc('day', call_block_time) as date) as block_date
        , call_block_slot as block_slot
        , 'burn' as action
        , amount
        , account_account as from_token_account
        , cast(null as varchar) as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM 
        {{ source('spl_token_solana','spl_token_call_burnChecked') }}
    WHERE 
        1=1
        {% if is_incremental() -%}
        AND {{incremental_predicate('call_block_time')}}
        {% endif -%}
)
, transfers AS (
    SELECT
        block_time
        , block_date
        , block_slot
        , action
        , amount
        , substring(from_token_account, 1, 2) as from_token_account_prefix
        , from_token_account
        , substring(to_token_account, 1, 2) as to_token_account_prefix
        , to_token_account
        , token_version
        , tx_signer
        , tx_id
        , tx_index
        , outer_instruction_index
        , inner_instruction_index
        , outer_executing_account
        , concat(
            lpad(cast(block_slot as varchar), 12, '0'), '-',
            lpad(cast(tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(inner_instruction_index, 0) as varchar), 4, '0')
        ) as unique_instruction_key --block time is not granular enough, build unique key from block_slot, tx_index, outer_instruction_index, inner_instruction_index
    FROM
        transfers_raw
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