{{
  config(
    schema = 'raydium_v4_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2021-03-21' %}

WITH swaps AS (
    SELECT
          account_amm
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_amm_solana', 'raydium_amm_call_swapBaseOut') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          account_amm
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_amm_solana', 'raydium_amm_call_swapBaseIn') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
    UNION ALL

    -- AMM V2 swaps are not registered in the decoded sources above.
    -- Raydium's eight-account layout uses account 2 for the pool and emits
    -- the same input/output SPL transfers consumed by the base trade model.
    -- Opcodes 16/17 are disjoint from legacy swapBaseIn/Out (9/11).
    -- https://github.com/raydium-io/raydium-amm/blob/master/program/src/instruction.rs
    SELECT
          element_at(account_arguments, 2) AS account_amm
        , is_inner AS call_is_inner
        , outer_instruction_index AS call_outer_instruction_index
        , inner_instruction_index AS call_inner_instruction_index
        , tx_id AS call_tx_id
        , block_time AS call_block_time
        , block_slot AS call_block_slot
        , block_date AS call_block_date
        , outer_executing_account AS call_outer_executing_account
        , tx_signer AS call_tx_signer
        , tx_index AS call_tx_index
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE executing_account = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
        AND executing_account_prefix = '67'
        AND tx_success
        AND bytearray_substring(data, 1, 1) IN (0x10, 0x11)
        AND length(data) = 17
        AND cardinality(account_arguments) >= 8
        AND element_at(account_arguments, 1) = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
)

-- Overlapping upstream IDL ranges can emit identical swaps; collapse them before MERGE.
SELECT DISTINCT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_date) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_is_inner AS is_inner
    , sp.call_tx_id AS tx_id
    , sp.call_tx_signer AS tx_signer
    , sp.call_tx_index AS tx_index
    , sp.account_amm AS pool_id
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps sp
