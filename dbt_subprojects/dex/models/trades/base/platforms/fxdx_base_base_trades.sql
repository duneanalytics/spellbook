{{
    config(
        schema = 'fxdx_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

WITH swaps AS (
    SELECT
        s.evt_block_number AS block_number,
        s.evt_block_time AS block_time,
        s.account,
        s.amountIn,
        s.amountOut,
        s.amountOutAfterFees,
        s.feeBasisPoints,
        s.tokenIn,
        s.tokenOut,
        s.contract_address AS project_contract_address,
        s.evt_tx_hash AS tx_hash,
        s.evt_index
    FROM {{ source('fxdx_base', 'vault_evt_swap') }} s
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain,
    'fxdx' AS project,
    '1' AS version,
    CAST(date_trunc('month', swaps.block_time) AS date) AS block_month,
    CAST(date_trunc('day', swaps.block_time) AS date) AS block_date,
    swaps.block_time,
    swaps.block_number,
    swaps.amountIn AS token_sold_amount_raw,
    swaps.amountOut AS token_bought_amount_raw,
    swaps.tokenIn AS token_sold_address,
    swaps.tokenOut AS token_bought_address,
    swaps.account AS taker,
    swaps.project_contract_address AS maker,
    swaps.tx_hash,
    swaps.evt_index
FROM swaps
