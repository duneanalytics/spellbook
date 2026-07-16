{{ config(
    schema = 'ferro_cronos'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Ferro is a Saddle/StableSwap fork. The TokenSwap event only exposes the pool-local
-- token indexes (soldId / boughtId), so we resolve them to token addresses using the
-- pooledTokens array emitted by the SwapDeployer at pool creation time.
WITH pool_tokens AS (
    SELECT
        swapAddress AS pool_address
        , token_address
        , CAST(token_index - 1 AS int256) AS token_id -- pooledTokens ordinality is 1-based, TokenSwap ids are 0-based
    FROM
        {{ source('ferro_cronos', 'SwapDeployer_evt_NewSwapPool') }}
    CROSS JOIN UNNEST(pooledTokens) WITH ORDINALITY AS t(token_address, token_index)
)

, token_swaps AS (
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.tokensBought AS token_bought_amount_raw
        , t.tokensSold AS token_sold_amount_raw
        , CAST(t.boughtId AS int256) AS bought_id
        , CAST(t.soldId AS int256) AS sold_id
        , t.buyer AS taker
        , CAST(NULL AS varbinary) AS maker
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        {{ source('ferro_cronos', 'Swap_evt_TokenSwap') }} t
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

, dexs AS (
    SELECT
        s.block_number
        , s.block_time
        , s.token_bought_amount_raw
        , s.token_sold_amount_raw
        , pt_bought.token_address AS token_bought_address
        , pt_sold.token_address AS token_sold_address
        , s.taker
        , s.maker
        , s.project_contract_address
        , s.tx_hash
        , s.evt_index
    FROM
        token_swaps s
    INNER JOIN pool_tokens pt_bought
        ON s.project_contract_address = pt_bought.pool_address
        AND s.bought_id = pt_bought.token_id
    INNER JOIN pool_tokens pt_sold
        ON s.project_contract_address = pt_sold.pool_address
        AND s.sold_id = pt_sold.token_id
)

SELECT
    'cronos' AS blockchain
    , 'ferro' AS project
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
