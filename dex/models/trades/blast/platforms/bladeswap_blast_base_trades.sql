{{
    config(
        schema = 'bladeswap_blast',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH source_expanded AS (
    SELECT
        delta[1] as amount0,
        delta[2] as amount1,
        bytearray_substring (tokenRef[1], 13, 20) as token0,
        bytearray_substring (tokenRef[2], 13, 20) as token1,
        *
    FROM
        {{ source('bladeswap_blast', 'Vault_Router_evt_Swap') }}
    WHERE CARDINALITY(delta) > 1
    {% if is_incremental() %}
      AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}

)
, dexs AS (
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.user AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN t.token0 ELSE t.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN t.token1 ELSE t.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.pool AS pool
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        source_expanded t

)

SELECT
    'blast' AS blockchain
    , 'bladeswap' AS project
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
