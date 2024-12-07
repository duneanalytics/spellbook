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
        t.contract_address,
        t.evt_block_time AS block_time,
        t.evt_block_number AS block_number,
        t.to AS token_bought_address,
        t.sender AS token_sold_address,
        CAST(0 AS DECIMAL(38, 0)) AS token_bought_amount_raw, -- Fixed as 0
        CAST(0 AS DECIMAL(38, 0)) AS token_sold_amount_raw -- Fixed as 0
    FROM {{ source('swapline_base', 'LBPair_evt_Swap') }} t
),
pair_creation_events AS (
    SELECT
        t.evt_tx_hash AS tx_hash,
        t.evt_index,
        t.contract_address,
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
    swap.contract_address AS project_contract_address,
    swap.block_time AS block_time,
    CAST(date_trunc('month', swap.block_time) AS date) AS block_month,
    CAST(date_trunc('day', swap.block_time) AS date) AS block_date,
    swap.tx_hash,
    swap.evt_index,
    COALESCE(swap.token_bought_address, pair.token_bought_address) AS token_bought_address,
    COALESCE(swap.token_sold_address, pair.token_sold_address) AS token_sold_address,
    CAST(NULL AS VARBINARY) AS taker,
    CAST(NULL AS VARBINARY) AS maker,
    swap.block_number,
    swap.token_bought_amount_raw, -- Fixed as 0 in swap_events
    swap.token_sold_amount_raw -- Fixed as 0 in swap_events
FROM swap_events AS swap
LEFT JOIN pair_creation_events AS pair
    ON swap.tx_hash = pair.tx_hash
    AND swap.evt_index = pair.evt_index
