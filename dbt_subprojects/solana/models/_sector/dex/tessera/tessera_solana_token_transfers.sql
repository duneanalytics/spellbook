{{
  config(
    schema = 'tessera_solana'
    , alias = 'token_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_date', 'unique_instruction_key']
  )
}}

{% set project_start_date = '2025-06-12' %}

-- Base swaps 
WITH tessera_swaps AS (
    SELECT
          block_slot
        , block_date
        , outer_instruction_index
        , tx_index
    FROM {{ source('solana','instruction_calls') }}
    WHERE 1=1
        AND executing_account = 'TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH'
        AND BYTEARRAY_SUBSTRING(data, 1, 1) = 0x10 -- Swap tag/discriminator. See: https://dune.com/queries/5857473
        AND tx_success = true 
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)
, token_transfers AS (
    select * 
    from {{ source('tokens_solana','transfers') }}
    where 1=1
        AND token_version != 'native'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

select *
from token_transfers
inner join tessera_swaps using (block_date, block_slot, tx_index, outer_instruction_index)