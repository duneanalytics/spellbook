{{
  config(
        schema = 'jupiter_v6_solana',
        alias = 'swapsevent_aggregator_swaps',
        partition_by = ['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month','amm','outer_instruction_index','inner_instruction_index','tx_id','output_mint','input_mint'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
    )
}}

{% set project_start_date = '2024-10-01' %}

WITH route_calls AS (
    SELECT
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_exact_out_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_time,
        call_block_slot,
        call_tx_index,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS call_inner_instruction_index
    FROM {{ source('jupiter_v6_solana', 'jupiter_call_shared_accounts_route_v2') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
        {% else %}
        call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
),

amm_list AS (
    SELECT DISTINCT amm
    FROM {{ source('jupiter_v6_solana', 'jupiter_evt_swapevent') }}
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('evt_block_time') }}
        {% else %}
        evt_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
),

amms_involved AS (
    SELECT
        a.block_slot,
        a.tx_index,
        a.executing_account AS amm,
        a.outer_instruction_index,
        a.inner_instruction_index,
        RANK() OVER (
            PARTITION BY a.block_slot, a.tx_index, b.call_outer_instruction_index
            ORDER BY a.outer_instruction_index ASC, a.inner_instruction_index ASC
        ) AS rnk
    FROM {{ source('solana', 'instruction_calls') }} a
    INNER JOIN route_calls b
        ON a.block_slot = b.call_block_slot
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
),

swap_amounts AS (
    SELECT
        evt_block_slot AS block_slot,
        evt_tx_index AS tx_index,
        evt_block_time AS block_time,
        evt_tx_id AS tx_id,
        evt_outer_instruction_index AS outer_instruction_index,
        ROW_NUMBER() OVER (
            PARTITION BY evt_block_slot, evt_tx_index, evt_outer_instruction_index
            ORDER BY ord ASC
        ) AS swap_order,
        REPLACE(REPLACE(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.input_mint'), 'PublicKey(', ''), ')', '') AS input_mint,
        CAST(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.input_amount') AS UINT256) AS input_amount,
        REPLACE(REPLACE(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.output_mint'), 'PublicKey(', ''), ')', '') AS output_mint,
        CAST(json_extract_scalar(CAST(evt AS VARCHAR), '$.SwapEventV2.output_amount') AS UINT256) AS output_amount
    FROM {{ source('jupiter_v6_solana', 'jupiter_evt_swapsevent') }}
    CROSS JOIN UNNEST(swap_events) WITH ORDINALITY AS s(evt, ord)
    WHERE
        {% if is_incremental() %}
        {{ incremental_predicate('evt_block_time') }}
        {% else %}
        evt_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
),

swaps_with_amm AS (
    SELECT
        a.block_time,
        a.block_slot,
        a.tx_index,
        a.tx_id,
        b.amm,
        b.outer_instruction_index,
        b.inner_instruction_index,
        a.input_mint,
        a.input_amount,
        a.output_mint,
        a.output_amount
    FROM swap_amounts a
    INNER JOIN amms_involved b
        ON a.block_slot = b.block_slot
        AND a.tx_index = b.tx_index
        AND a.outer_instruction_index = b.outer_instruction_index
        AND a.swap_order = b.rnk
),

amms AS (
    SELECT * FROM {{ ref('jupiter_solana_amms') }}
)

SELECT
    l.amm,
    am.amm_name,
    CASE
        WHEN l.input_mint > l.output_mint THEN tk_1.symbol || '-' || tk_2.symbol
        ELSE tk_2.symbol || '-' || tk_1.symbol
    END AS token_pair,
    tk_1.symbol AS input_symbol,
    l.input_mint,
    l.input_amount,
    tk_1.decimals AS input_decimals,
    l.input_amount / pow(10, p_1.decimals) * p_1.price AS input_usd,
    tk_2.symbol AS output_symbol,
    l.output_mint,
    l.output_amount,
    l.output_amount / pow(10, p_2.decimals) * p_2.price AS output_usd,
    tk_2.decimals AS output_decimals,
    l.outer_instruction_index,
    l.inner_instruction_index,
    l.tx_id,
    l.block_slot,
    l.block_time,
    CAST(date_trunc('month', l.block_time) AS DATE) AS block_month,
    6 AS jup_version
FROM swaps_with_amm l
LEFT JOIN amms am ON am.amm = l.amm
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_1 ON tk_1.token_mint_address = l.input_mint
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_2 ON tk_2.token_mint_address = l.output_mint
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_1
    ON p_1.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_1.minute
    AND l.input_mint = toBase58(p_1.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_1.minute') }}
    {% else %}
    AND p_1.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_2
    ON p_2.blockchain = 'solana'
    AND date_trunc('minute', l.block_time) = p_2.minute
    AND l.output_mint = toBase58(p_2.contract_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_2.minute') }}
    {% else %}
    AND p_2.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
WHERE l.input_mint NOT IN ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')
    AND l.output_mint NOT IN ('4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi')
