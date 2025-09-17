{{
  config(
    schema = 'solfi_solana'
    , alias = 'token_transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'block_time', 'unique_instruction_key']
  )
}}

{% set project_start_date = '2025-08-01' %}

-- Base swaps from solfi_call_swap table
WITH solfi_swaps AS (
    SELECT distinct
        call_block_time as block_time
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
    FROM {{ source('solfi_solana', 'solfi_call_swap') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
    {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)
, token_transfers AS (
    select * 
    from {{ source('tokens_solana','transfers') }}
    where 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

select *
from token_transfers
inner join solfi_swaps using (block_time, block_slot, tx_index, outer_instruction_index)