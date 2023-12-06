{{
    config(
        schema = 'pancakeswap_v2_bnb',
        alias = 'stableswap_base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    -- PancakeSwap v2 stableswap
    SELECT
        t.evt_block_time                                                                AS block_time,
        t.buyer                                                                         AS taker, 
        CAST(NULL AS VARBINARY)                                                         AS maker,
        tokens_bought                                                                   AS token_bought_amount_raw,
        tokens_sold                                                                     AS token_sold_amount_raw,
        CASE WHEN bought_id = UINT256 '0' THEN f.tokenA ELSE f.tokenB END               AS token_bought_address,
        CASE WHEN bought_id = UINT256 '0' THEN f.tokenB ELSE f.tokenA END               AS token_sold_address,
        t.contract_address                                                              AS project_contract_address,
        t.evt_tx_hash                                                                   AS tx_hash,
        t.evt_index
    FROM
        (
        SELECT * FROM {{ source('pancakeswap_v2_bnb', 'PancakeStableSwap_evt_TokenExchange') }}
        UNION ALL
        SELECT * FROM {{ source('pancakeswap_v2_bnb', 'PancakeStableSwapTwoPool_evt_TokenExchange') }}   
        ) t
    INNER JOIN (
            SELECT a.*
            FROM {{ source('pancakeswap_v2_bnb', 'PancakeStableSwapFactory_evt_NewStableSwapPair') }} a
            INNER JOIN (
              SELECT swapContract, MAX(evt_block_time) AS latest_time
              FROM {{ source('pancakeswap_v2_bnb', 'PancakeStableSwapFactory_evt_NewStableSwapPair') }}
              GROUP BY swapContract
            ) b
            ON a.swapContract = b.swapContract AND a.evt_block_time = b.latest_time
        ) f
    ON t.contract_address = f.swapContract
    {% if is_incremental() %}
    AND {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'bnb'                                                       AS blockchain
    , 'pancakeswap'                                             AS project
    , 'stableswap'                                              AS version
    , CAST(date_trunc('month', dexs.block_time) AS date)        AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date)          AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM dexs