{{
    config(
        schema = 'iziswap_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set iziswap_start_date = "2023-03-28" %}

WITH
    trades AS (
        SELECT
            contract_address AS project_contract_address
            , '1' AS version
            , evt_tx_hash AS tx_hash
            , evt_index
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , CASE WHEN sellXEarnY THEN tokenY ELSE tokenX END AS token_bought_address 
            , CASE WHEN sellXEarnY THEN tokenX ELSE tokenY END AS token_sold_address
            , CASE WHEN sellXEarnY THEN amountY ELSE amountX END AS token_bought_amount_raw 
            , CASE WHEN sellXEarnY THEN amountX ELSE amountY END AS token_sold_amount_raw
        FROM {{ source('iziswap_v1_zksync', 'iZiSwapPool_evt_Swap') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{iziswap_start_date}}'
        {% endif %}
        UNION
        SELECT
            contract_address AS project_contract_address
            , '2' AS version
            , evt_tx_hash AS tx_hash
            , evt_index
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , CASE WHEN sellXEarnY THEN tokenY ELSE tokenX END AS token_bought_address 
            , CASE WHEN sellXEarnY THEN tokenX ELSE tokenY END AS token_sold_address
            , CASE WHEN sellXEarnY THEN amountY ELSE amountX END AS token_bought_amount_raw 
            , CASE WHEN sellXEarnY THEN amountX ELSE amountY END AS token_sold_amount_raw
        FROM {{ source('iziswap_v2_zksync', 'iZiSwapPool_evt_Swap') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{iziswap_start_date}}'
        {% endif %}
    )

SELECT
    'zksync' AS blockchain
    , 'iziswap' AS project
    , version
    , CAST(DATE_TRUNC('month', block_time) AS DATE) AS block_month
    , CAST(DATE_TRUNC('day', block_time) AS DATE) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , CAST(NULL AS VARBINARY) AS taker
    , CAST(NULL AS VARBINARY) AS maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM trades
