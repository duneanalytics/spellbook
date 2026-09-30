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
    , pre_hook = [
        "{{ enforce_join_distribution('PARTITIONED') }}"
        , "{{ set_trino_session_property(true, 'join_reordering_strategy', 'NONE') }}"
      ]
  )
}}

{% set project_start_date = '2024-06-05' %}
{% set ci_start_date = '2026-09-01' %}

WITH whirlpool_v2_swaps AS (
    SELECT DISTINCT
        block_date, block_slot, tx_index, outer_instruction_index
    FROM (
        SELECT
              call_block_date AS block_date
            , call_block_slot AS block_slot
            , call_tx_index AS tx_index
            , call_outer_instruction_index AS outer_instruction_index
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_swapV2', 'whirlpool_call_swap_v2', [], bounded=true, dedupe=false) }}) decoded_swap_v2

        UNION ALL

        SELECT
              call_block_date AS block_date
            , call_block_slot AS block_slot
            , call_tx_index AS tx_index
            , call_outer_instruction_index AS outer_instruction_index
        FROM ({{ orca_whirlpool_decoded_calls('whirlpool_call_twoHopSwapV2', 'whirlpool_call_two_hop_swap_v2', [], bounded=true, dedupe=false) }}) decoded_two_hop
    ) swaps
    {% if target.name == 'ci' -%}
    WHERE block_date >= DATE '{{ ci_start_date }}'
    {% endif -%}
)

, token_transfers AS (
    SELECT
          block_date
        , block_slot
        , tx_index
        , tx_id
        , outer_instruction_index
        , inner_instruction_index
        , unique_instruction_key
        , amount
        , token_mint_address
        , from_token_account
        , to_token_account
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE 1=1
        AND token_version != 'native'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
        {% if target.name == 'ci' -%}
        -- Full history does not finish inside the CI time limit. 2026-09-01 keeps the seed rows.
        AND block_date >= DATE '{{ ci_start_date }}'
        {% endif -%}
)

-- Swaps stay on the right. With join reordering off, that side is the one loaded into memory.
SELECT
      t.block_date
    , t.block_slot
    , t.tx_index
    , t.tx_id
    , t.outer_instruction_index
    , t.inner_instruction_index
    , t.unique_instruction_key
    , t.amount
    , t.token_mint_address
    , t.from_token_account
    , t.to_token_account
FROM token_transfers AS t
INNER JOIN whirlpool_v2_swaps AS w
    ON t.block_date = w.block_date
    AND t.block_slot = w.block_slot
    AND t.tx_index = w.tx_index
    AND t.outer_instruction_index = w.outer_instruction_index
