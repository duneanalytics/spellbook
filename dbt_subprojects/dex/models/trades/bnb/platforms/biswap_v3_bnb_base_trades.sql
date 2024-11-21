{{
    config(
        schema = 'biswap_v3_bnb',
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
        CAST(NULL AS VARBINARY) AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        CASE WHEN sellXEarnY = true THEN abs(amountY) ELSE abs(amountX) END AS token_bought_amount_raw,
        CASE WHEN sellXEarnY = true THEN abs(amountX) ELSE abs(amountY) END AS token_sold_amount_raw,
        CASE WHEN sellXEarnY = true THEN tokenY ELSE tokenX END AS token_bought_address,
        CASE WHEN sellXEarnY = true THEN tokenX ELSE tokenY END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM (
        SELECT
            a.*,
            CAST(json_extract_scalar(returnValues, '$.amountX') AS uint256) AS amountX,
            CAST(json_extract_scalar(returnValues, '$.amountY') AS uint256) AS amountY
        FROM {{ source('biswap_v3_bnb', 'BiswapPoolV3_evt_Swap') }} a
    ) t 
    INNER JOIN {{ source('biswap_v3_bnb', 'BiswapFactoryV3_evt_NewPool') }} f ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT
    'bnb' AS blockchain,
    'biswap' AS project,
    '3' AS version,
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
