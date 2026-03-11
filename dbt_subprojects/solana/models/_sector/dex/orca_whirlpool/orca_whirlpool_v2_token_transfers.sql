{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'token_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_date', 'unique_instruction_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2024-06-05' %}

WITH whirlpool_v2_swaps AS (
    SELECT DISTINCT
        block_date, block_slot, tx_index, outer_instruction_index
    FROM (
        SELECT
              call_block_date AS block_date
            , call_block_slot AS block_slot
            , call_tx_index AS tx_index
            , call_outer_instruction_index AS outer_instruction_index
        FROM {{ source('whirlpool_solana', 'whirlpool_call_swapV2') }}
        WHERE 1=1
        {% if is_incremental() -%}
            AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
            AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}

        UNION ALL

        SELECT
              call_block_date AS block_date
            , call_block_slot AS block_slot
            , call_tx_index AS tx_index
            , call_outer_instruction_index AS outer_instruction_index
        FROM {{ source('whirlpool_solana', 'whirlpool_call_twoHopSwapV2') }}
        WHERE 1=1
        {% if is_incremental() -%}
            AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
            AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
    )
)

, token_transfers AS (
    SELECT *
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE 1=1
        AND token_version != 'native'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

SELECT *
FROM token_transfers
INNER JOIN whirlpool_v2_swaps USING (block_date, block_slot, tx_index, outer_instruction_index)
