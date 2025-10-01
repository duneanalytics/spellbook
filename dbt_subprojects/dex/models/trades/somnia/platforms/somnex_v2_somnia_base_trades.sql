{{
    config(
        schema = 'somnex_v2_somnia',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set somnex_start_date = "2025-08-29" %}

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.to AS taker,
        t.contract_address AS maker,
        -- amount1 fields are inflated by 1e10 ONLY for 18-decimal tokenA pairs
        -- For 6-decimal tokenA (like USDC), amount1 is already correct
        CASE 
            WHEN amount0Out = UINT256 '0' AND COALESCE(tokena_decimals.decimals, 18) = 18 
                THEN amount1Out / 1e10
            WHEN amount0Out = UINT256 '0' 
                THEN amount1Out
            ELSE amount0Out 
        END AS token_bought_amount_raw,
        CASE 
            WHEN (amount0In = UINT256 '0' OR amount1Out = UINT256 '0') AND COALESCE(tokena_decimals.decimals, 18) = 18 
                THEN amount1In / 1e10
            WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' 
                THEN amount1In
            ELSE amount0In 
        END AS token_sold_amount_raw,
        CASE WHEN amount0Out = UINT256 '0' THEN f.tokenb ELSE f.tokena END AS token_bought_address,
        CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.tokenb ELSE f.tokena END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index AS evt_index
    FROM {{ source('somnex_somnia', 'somnexammpair_evt_swap') }} t
    INNER JOIN {{ source('somnex_somnia', 'somnexammfactory_call_createpair') }} f
        ON f.output_pair = t.contract_address
    LEFT JOIN {{ source('tokens', 'erc20') }} tokena_decimals
        ON tokena_decimals.contract_address = f.tokena 
        AND tokena_decimals.blockchain = 'somnia'
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{somnex_start_date}}'
    {% endif %}
)

SELECT
    'somnia' AS blockchain,
    'somnex' AS project,
    '2' AS version,
    CAST(DATE_TRUNC('month', dexs.block_time) AS DATE) AS block_month,
    CAST(DATE_TRUNC('day', dexs.block_time) AS DATE) AS block_date,
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