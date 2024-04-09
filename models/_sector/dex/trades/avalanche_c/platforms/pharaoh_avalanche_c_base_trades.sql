{{ config(
    schema = 'pharaoh_avalanche_c'
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
        ,t.recipient AS taker
        ,CAST(NULL AS VARBINARY) AS maker
        ,CASE
            WHEN router.evt_tx_hash IS NULL
                THEN CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END
                ELSE router.amountOut
            END AS token_bought_amount_raw
        ,CASE
            WHEN router.evt_tx_hash IS NULL
                THEN CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END
                ELSE router.inputAmount
            END AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,CASE
            WHEN router.evt_tx_hash IS NULL
                THEN CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END
                ELSE router.outputToken
            END AS token_bought_address
        ,CASE
            WHEN router.evt_tx_hash IS NULL
                THEN CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END
                ELSE router.inputToken
            END AS token_sold_address
        ,t.contract_address as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index
        ,t.evt_block_number AS block_number
    FROM
        {{ source('pharaoh_avalanche_c', 'ClPool_evt_Swap') }} t
    INNER JOIN
        {{ source('pharaoh_avalanche_c', 'ClPoolFactory_evt_PoolCreated') }} f
        ON f.pool = t.contract_address
    LEFT JOIN {{ source('odos_v2_avalanche_c', 'OdosRouterV2_evt_Swap') }} AS router
        ON t.evt_tx_hash = router.evt_tx_hash
        AND t.evt_index + 2 = router.evt_index
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'avalanche_c' AS blockchain
    , 'pharaoh' AS project
    , '1' AS version
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
