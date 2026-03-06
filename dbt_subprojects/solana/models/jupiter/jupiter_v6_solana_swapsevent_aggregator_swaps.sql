{{
  config(
    schema = 'jupiter_v6_solana'
    , alias = 'swapsevent_aggregator_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'tx_id', 'outer_instruction_index', 'inner_instruction_index']
    , pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-09-17' %}

WITH amms AS (
    SELECT * FROM {{ ref('jupiter_solana_amms') }}
)

, route_calls AS (
    SELECT
          call_block_date
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_date') }}
        {% else %}
        call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_date
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_date') }}
        {% else %}
        call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_date
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_date') }}
        {% else %}
        call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_date
        , call_block_slot
        , call_tx_index
        , call_outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_date') }}
        {% else %}
        call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

, amm_list AS (
    SELECT
        amm
    FROM {{ source('jupiter_v6_solana', 'jupiter_evt_swapevent') }}
    GROUP BY
        amm
)

, amms_involved AS (
    SELECT
          a.block_date
        , a.block_slot
        , a.tx_index
        , a.executing_account AS amm
        , a.outer_instruction_index
        , a.inner_instruction_index
        , ROW_NUMBER() OVER (
            PARTITION BY a.block_date, a.tx_id, a.outer_instruction_index
            ORDER BY a.inner_instruction_index ASC
          ) AS rnk
    FROM {{ source('solana', 'instruction_calls') }} a
    INNER JOIN route_calls b
        ON  a.block_date = b.call_block_date
        AND a.block_slot = b.call_block_slot
        AND a.tx_index = b.call_tx_index
        AND a.outer_instruction_index = b.call_outer_instruction_index
        AND a.inner_instruction_index > b.call_inner_instruction_index
    INNER JOIN amm_list AS amm
        ON a.executing_account = amm.amm
    WHERE cardinality(a.account_arguments) >= 5
      {% if is_incremental() %}
      AND {{ incremental_predicate('a.block_date') }}
      {% else %}
      AND a.block_date >= DATE '{{ project_start_date }}'
      {% endif %}
)

, swap_amounts AS (
    SELECT
          evt_block_date AS block_date
        , evt_block_slot AS block_slot
        , evt_tx_index AS tx_index
        , evt_block_time AS block_time
        , evt_tx_id AS tx_id
        , evt_tx_signer AS tx_signer
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
        {{ incremental_predicate('evt_block_date') }}
        {% else %}
        evt_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

, joined AS (
    SELECT
          a.block_time
        , CAST(date_trunc('month', a.block_time) AS DATE) AS block_month
        , a.block_date
        , a.block_slot
        , a.tx_index
        , a.tx_id
        , a.tx_signer
        , b.amm
        , b.outer_instruction_index
        , b.inner_instruction_index
        , a.swap_order AS log_index
        , a.input_mint
        , a.input_amount
        , a.output_mint
        , a.output_amount
    FROM swap_amounts a
    INNER JOIN amms_involved b
        ON  a.block_date = b.block_date
        AND a.block_slot = b.block_slot
        AND a.tx_index = b.tx_index
        AND a.outer_instruction_index = b.outer_instruction_index
        AND a.swap_order = b.rnk
)

SELECT
      j.block_month
    , j.block_date
    , j.block_time
    , j.block_slot
    , j.tx_index
    , j.tx_id
    , j.tx_signer
    , j.amm
    , amms.amm_name
    , CASE WHEN j.input_mint > j.output_mint THEN tk_1.symbol || '-' || tk_2.symbol
        ELSE tk_2.symbol || '-' || tk_1.symbol
        END AS token_pair
    , j.outer_instruction_index
    , j.inner_instruction_index
    , j.log_index
    , tk_1.symbol AS input_symbol
    , j.input_mint
    , j.input_amount
    , tk_1.decimals AS input_decimals
    , j.input_amount / pow(10, p_1.decimals) * p_1.price AS input_usd
    , tk_2.symbol AS output_symbol
    , j.output_mint
    , j.output_amount
    , tk_2.decimals AS output_decimals
    , j.output_amount / pow(10, p_2.decimals) * p_2.price AS output_usd
    , 6 AS jup_version
FROM joined j
LEFT JOIN amms ON amms.amm = j.amm
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_1 ON tk_1.token_mint_address = j.input_mint
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_2 ON tk_2.token_mint_address = j.output_mint
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_1 ON p_1.blockchain = 'solana'
    AND date_trunc('minute', j.block_time) = p_1.minute
    AND j.input_mint = toBase58(p_1.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_1.minute') }}
    {% else %}
    AND p_1.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_2 ON p_2.blockchain = 'solana'
    AND date_trunc('minute', j.block_time) = p_2.minute
    AND j.output_mint = toBase58(p_2.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_2.minute') }}
    {% else %}
    AND p_2.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
