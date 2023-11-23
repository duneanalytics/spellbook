{{
    config(
        schema = 'arbswap_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    -- Arbswap AMM
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.to AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
        CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw,
        CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address,
        CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM
        {{ source('arbswap_arbitrum', 'SwapPair_evt_Swap') }} t
    INNER JOIN {{ source('arbswap_arbitrum', 'SwapFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
    
    UNION ALL 

    -- Arbswap Stableswap
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.buyer AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CASE WHEN bought_id = UINT256 '0' THEN f.tokenA ELSE f.tokenB END AS token_bought_address,
        CASE WHEN sold_id = UINT256 '0' THEN f.tokenA ELSE f.tokenB END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM
        {{ source('arbswap_arbitrum', 'ArbswapStableSwapTwoPool_evt_TokenExchange') }} t
    INNER JOIN {{ source('arbswap_arbitrum', 'ArbswapStableSwapFactory_evt_NewStableSwapPair') }} f
        ON f.swapContract = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'arbswap' AS project,
    '1' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
