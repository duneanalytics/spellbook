{{
  config(
    schema = 'jupiter_v6_solana'
    , alias = 'swapsevent_aggregator_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'amm', 'outer_instruction_index', 'inner_instruction_index', 'tx_id', 'output_mint', 'input_mint']
    , pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-09-17' %}

WITH route_calls AS (
    SELECT
          call_block_time
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

, amm_list AS (
    SELECT DISTINCT amm
    FROM {{ source('jupiter_v6_solana', 'jupiter_evt_swapevent') }}
    WHERE evt_block_date >= DATE '2025-09-17'
)

, amms_involved AS (
    SELECT
          a.block_slot
        , a.tx_index
        , a.executing_account AS amm
        , a.outer_instruction_index
        , a.inner_instruction_index
        , RANK() OVER (
            PARTITION BY a.block_slot, a.tx_index, b.call_outer_instruction_index
            ORDER BY a.outer_instruction_index ASC, a.inner_instruction_index ASC
          ) AS rnk
    FROM {{ source('solana', 'instruction_calls') }} a
    INNER JOIN route_calls b
        ON  a.block_slot = b.call_block_slot
        AND a.tx_index = b.call_tx_index
        AND a.outer_instruction_index = b.call_outer_instruction_index
        AND a.inner_instruction_index > b.call_inner_instruction_index
    WHERE a.executing_account IN (SELECT amm FROM amm_list)
      AND cardinality(a.account_arguments) >= 5
      {% if is_incremental() %}
      AND {{ incremental_predicate('a.block_time') }}
      {% else %}
      AND a.block_time >= TIMESTAMP '{{ project_start_date }}'
      {% endif %}
)

, swap_amounts AS (
    SELECT
          evt_block_slot AS block_slot
        , evt_tx_index AS tx_index
        , evt_block_time AS block_time
        , evt_tx_id AS tx_id
        , evt_outer_instruction_index AS outer_instruction_index
        , ROW_NUMBER() OVER (
            PARTITION BY evt_block_slot, evt_tx_index, evt_outer_instruction_index
            ORDER BY ord ASC
          ) AS swap_order
        , REPLACE(REPLACE(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.input_mint'), 'PublicKey(', ''), ')', '') AS input_mint
        , CAST(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.input_amount') AS UINT256) AS input_amount
        , REPLACE(REPLACE(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.output_mint'), 'PublicKey(', ''), ')', '') AS output_mint
        , CAST(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.output_amount') AS UINT256) AS output_amount
    FROM {{ source('jupiter_v6_solana', 'jupiter_evt_swapsevent') }}
    CROSS JOIN UNNEST(swap_events) WITH ORDINALITY AS s(evt, ord)
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('evt_block_time') }}
        {% else %}
        evt_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

, joined AS (
    SELECT
          a.block_time
        , CAST(date_trunc('month', a.block_time) AS DATE) AS block_month
        , a.block_slot
        , a.tx_index
        , a.tx_id
        , b.amm
        , b.outer_instruction_index
        , b.inner_instruction_index
        , a.input_mint
        , a.input_amount
        , a.output_mint
        , a.output_amount
    FROM swap_amounts a
    INNER JOIN amms_involved b
        ON  a.block_slot = b.block_slot
        AND a.tx_index = b.tx_index
        AND a.outer_instruction_index = b.outer_instruction_index
        AND a.swap_order = b.rnk
)

SELECT
      max(block_time) AS block_time
    , block_month
    , max(block_slot) AS block_slot
    , max(tx_index) AS tx_index
    , tx_id
    , amm
    , outer_instruction_index
    , inner_instruction_index
    , input_mint
    , max(input_amount) AS input_amount
    , output_mint
    , max(output_amount) AS output_amount
FROM joined
GROUP BY
      block_month
    , amm
    , outer_instruction_index
    , inner_instruction_index
    , tx_id
    , input_mint
    , output_mint
