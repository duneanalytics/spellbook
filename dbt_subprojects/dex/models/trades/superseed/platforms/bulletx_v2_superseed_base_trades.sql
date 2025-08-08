{{ 
    config(
        schema = 'bulletx_v2_superseed',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.to AS taker,
        t.contract_address AS maker,
        CAST(
            CASE 
                WHEN amount0Out > UINT256 '0' THEN 
                    CASE WHEN f.tokenA < f.tokenB THEN f.tokenA ELSE f.tokenB END
                ELSE 
                    CASE WHEN f.tokenA < f.tokenB THEN f.tokenB ELSE f.tokenA END
            END AS VARBINARY
        ) AS token_bought_address,

        CAST(
            CASE 
                WHEN amount0In > UINT256 '0' THEN 
                    CASE WHEN f.tokenA < f.tokenB THEN f.tokenA ELSE f.tokenB END
                ELSE 
                    CASE WHEN f.tokenA < f.tokenB THEN f.tokenB ELSE f.tokenA END
            END AS VARBINARY
        ) AS token_sold_address,

        CAST(
            CASE WHEN amount0Out > UINT256 '0' THEN amount0Out ELSE amount1Out END
            AS UINT256
        ) AS token_bought_amount_raw,

        CAST(
            CASE WHEN amount0In > UINT256 '0' THEN amount0In ELSE amount1In END
            AS UINT256
        ) AS token_sold_amount_raw,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index AS evt_index
    FROM {{ source('bulletx_superseed', 'V2Pair_evt_Swap') }} t
    INNER JOIN {{ source('bulletx_superseed', 'BulletXFactory_call_createPair') }} f
        ON f.output_pair = t.contract_address
        AND f.call_success = true
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'superseed' AS blockchain,
    'bulletx' AS project,
    '2' AS version,
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
WHERE (token_bought_amount_raw IS NOT NULL AND token_bought_amount_raw > 0)
   OR (token_sold_amount_raw IS NOT NULL AND token_sold_amount_raw > 0)