{{ config(
    schema = 'trader_joe_v2_bnb'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    SELECT
        t.evt_block_time AS block_time
        ,t.evt_block_number AS block_number
        ,t.recipient AS taker
        ,CAST(NULL AS VARBINARY) AS maker
        ,amountOut AS token_bought_amount_raw
        ,amountIn AS token_sold_amount_raw
        ,CASE WHEN swapForY = true THEN f.tokenY ELSE f.tokenX END AS token_bought_address -- when swapForY is true it means that tokenY is the token being bought else it's tokenX
        ,CASE WHEN swapForY = true THEN f.tokenX ELSE f.tokenY END AS token_sold_address
        ,t.contract_address AS project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index
    FROM
        {{ source('trader_joe_v2_bnb', 'LBPair_evt_Swap') }} t
    INNER JOIN {{ source('trader_joe_v2_bnb', 'LBFactory_evt_LBPairCreated') }} f
        ON f.LBPair = t.contract_address
    {% if is_incremental() %}  -- comment to accomodate additions to prices.usd and force full reload
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'bnb' AS blockchain
    ,'trader_joe' AS project
    ,'2' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
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
FROM
    dexs