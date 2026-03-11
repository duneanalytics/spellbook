{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'token_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_date', 'unique_instruction_key']
  )
}}

{% set project_start_date = '2024-06-05' %}

WITH whirlpool_v2_swaps AS (
    SELECT DISTINCT
        block_date
        , block_slot
        , tx_index
        , outer_instruction_index
    FROM {{ ref('orca_whirlpool_v2_stg_swaps') }}
    WHERE 1=1
    {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
    {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
)

, token_transfers AS (
    SELECT *
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE 1=1
        AND token_version != 'native'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
)

SELECT *
FROM token_transfers
INNER JOIN whirlpool_v2_swaps USING (block_date, block_slot, tx_index, outer_instruction_index)
