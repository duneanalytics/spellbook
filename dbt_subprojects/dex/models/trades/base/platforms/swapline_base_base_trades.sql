{{
    config(
        schema = 'swapline_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH swap_events AS (
    SELECT
        t.evt_tx_hash AS tx_hash,
        t.evt_index,
        t.evt_block_time AS block_time,
        t.evt_block_number AS block_number,
        t.to AS token_bought_address,
        t.sender AS token_sold_address,
        t.amountsOut AS token_bought_amount_raw,
        t.amountsIn AS token_sold_amount_raw
    FROM {{ source('swapline_base', 'LBPair_evt_Swap') }} t
),
pair_creation_events AS (
    SELECT
        t.evt_tx_hash AS tx_hash,
        t.evt_index,
        t.evt_block_time AS block_time,
        t.evt_block_number AS block_number,
        t.tokenY AS token_bought_address,
        t.tokenX AS token_sold_address
    FROM {{ source('swapline_base', 'LBFactory_evt_LBPairCreated') }} t
)
SELECT DISTINCT
    'base' AS blockchain,
    'swapline' AS project,
    '1' AS version,
    swap.block_time AS block_time,
    CAST(date_trunc('month', swap.block_time) AS date) AS block_month,
    CAST(date_trunc('day', swap.block_time) AS date) AS block_date,
    swap.tx_hash,
    swap.evt_index,
    COALESCE(swap.token_bought_address, pair.token_bought_address) AS token_bought_address,
    COALESCE(swap.token_sold_address, pair.token_sold_address) AS token_sold_address,
    DATE_TRUNC('month', swap.block_time) AS block_month,
    swap.block_number,
    swap.token_bought_amount_raw,
    swap.token_sold_amount_raw
FROM swap_events AS swap
LEFT JOIN pair_creation_events AS pair
    ON swap.tx_hash = pair.tx_hash
    AND swap.evt_index = pair.evt_index
