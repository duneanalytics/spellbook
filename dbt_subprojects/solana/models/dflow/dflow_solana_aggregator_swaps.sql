{{
  config(
        schema = 'dflow_solana',
        alias = 'aggregator_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_id', 'input_mint', 'output_mint', 'input_amount', 'output_amount'],
        pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
    )
}}

{% set project_start_date = '2025-04-01' %}

with
    amms as (
        SELECT * FROM {{ ref('jupiter_solana_amms') }}
    )

    , dflow_swaps as (
        SELECT
            e.evt_block_time,
            e.evt_block_slot,
            e.evt_tx_id,
            e.evt_tx_signer,
            e.amm,
            a.amm_name,
            e.input_mint,
            CAST(e.input_amount AS BIGINT) as input_amount,
            e.output_mint,
            CAST(e.output_amount AS BIGINT) as output_amount,
            e.evt_inner_instruction_index,
            e.evt_outer_instruction_index
        FROM {{ source('dflow_solana', 'swap_orchestrator_evt_swapevent') }} e
        JOIN amms a ON a.amm = e.amm
        WHERE e.evt_block_time >= TIMESTAMP '{{ project_start_date }}'
            {% if is_incremental() -%}
            AND {{ incremental_predicate('e.evt_block_time') }}
            {% endif -%}
    )

SELECT
    s.evt_block_time as block_time,
    s.evt_block_slot as block_slot,
    s.evt_tx_id as tx_id,
    s.evt_tx_signer as tx_signer,
    'DFlow' as aggregator,
    s.amm,
    s.amm_name,
    s.input_mint,
    tk_1.symbol as input_symbol,
    tk_1.decimals as input_decimals,
    s.input_amount,
    s.input_amount / pow(10, tk_1.decimals) as input_amount_decimal,
    s.input_amount / pow(10, p_1.decimals) * p_1.price as input_usd,
    s.output_mint,
    tk_2.symbol as output_symbol,
    tk_2.decimals as output_decimals,
    s.output_amount,
    s.output_amount / pow(10, tk_2.decimals) as output_amount_decimal,
    s.output_amount / pow(10, p_2.decimals) * p_2.price as output_usd,
    s.evt_inner_instruction_index,
    s.evt_outer_instruction_index,
    CAST(date_trunc('month', s.evt_block_time) AS DATE) as block_month
FROM dflow_swaps s
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_1
    ON tk_1.token_mint_address = s.input_mint
LEFT JOIN {{ source('tokens_solana', 'fungible') }} tk_2
    ON tk_2.token_mint_address = s.output_mint
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_1 ON p_1.blockchain = 'solana'
    AND date_trunc('minute', s.evt_block_time) = p_1.minute
    AND s.input_mint = toBase58(p_1.contract_address)
    {% if is_incremental() -%}
    AND {{ incremental_predicate('p_1.minute') }}
    {% else -%}
    AND p_1.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
LEFT JOIN {{ source('prices', 'usd_forward_fill') }} p_2 ON p_2.blockchain = 'solana'
    AND date_trunc('minute', s.evt_block_time) = p_2.minute
    AND s.output_mint = toBase58(p_2.contract_address)
    {% if is_incremental() -%}
    AND {{ incremental_predicate('p_2.minute') }}
    {% else -%}
    AND p_2.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
WHERE s.input_amount > 0
    AND s.output_amount > 0