{{
    config(
        schema = 'curve_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- This should depend on 'curve_optimism_pools' running first
-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql
-- Start Time
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.StableSwap_evt_TokenExchange
-- UNION ALL
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.MetaPoolSwap_evt_TokenExchange

with dexs as
(
SELECT
    pool_type,
    block_time,
    block_number,
    taker,
    maker,
    token_bought_amount_raw,
    token_sold_amount_raw,
    ta.token AS token_bought_address,
    tb.token AS token_sold_address,
    project_contract_address,
    tx_hash,
    evt_index,
    bought_id,
    sold_id
    FROM (
        -- Stableswap
        SELECT
            'stable' AS pool_type, -- has implications for decimals for curve
            t.evt_block_time AS block_time,
            t.evt_block_number AS block_number,
            t.buyer AS taker,
            CAST(NULL as varbinary) AS maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought AS token_bought_amount_raw,
            tokens_sold AS token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index,
            bought_id,
            sold_id
        FROM {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- MetaPoolSwap TokenExchangeUnderlying
        SELECT
            'meta' AS pool_type, -- has implications for decimals for curve
            t.evt_block_time AS block_time,
            t.evt_block_number,
            t.buyer AS taker,
            CAST(NULL as varbinary) AS maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought AS token_bought_amount_raw,
            tokens_sold AS token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index,
            bought_id,
            sold_id
        FROM {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchangeUnderlying') }} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- StableSwap - Mislabeled as MetaPoolSwap TokenExchange
        SELECT
            'stable' AS pool_type, -- has implications for decimals for curve
            t.evt_block_time AS block_time,
            t.evt_block_number,
            t.buyer AS taker,
            CAST(NULL as varbinary) AS maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought AS token_bought_amount_raw,
            tokens_sold AS token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index, 
            bought_id, 
            sold_id
        FROM {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchange') }} t
        -- handle for dupes due to decoding issues
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchangeUnderlying') }} s 
            WHERE t.evt_block_number = s.evt_block_number
                AND t.evt_tx_hash = s.evt_tx_hash
                AND t.evt_index = s.evt_index
                {% if is_incremental() %}
                AND {{ incremental_predicate('s.evt_block_time') }}
                {% endif %}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} s 
            WHERE t.evt_block_number = s.evt_block_number
                AND t.evt_tx_hash = s.evt_tx_hash
                AND t.evt_index = s.evt_index
                {% if is_incremental() %}
                AND {{ incremental_predicate('s.evt_block_time') }}
                {% endif %}
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

        UNION ALL

        -- Stableswap
        SELECT
            'stable' AS pool_type, -- has implications for decimals for curve
            t.evt_block_time AS block_time,
            t.evt_block_number AS block_number,
            t.buyer AS taker,
            CAST(NULL as varbinary) AS maker,
            -- when amount0 is negative it means taker is buying token0 from the pool
            tokens_bought AS token_bought_amount_raw,
            tokens_sold AS token_sold_amount_raw,
            t.contract_address as project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index,
            bought_id,
            sold_id
        FROM {{ source('curvefi_optimism', 'wstETH_swap_evt_TokenExchange') }} t --Should be Stableswap, but mis-decoded
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} s 
            WHERE t.evt_block_number = s.evt_block_number
                AND t.evt_tx_hash = s.evt_tx_hash
                AND t.evt_index = s.evt_index
                {% if is_incremental() %}
                AND {{ incremental_predicate('s.evt_block_time') }}
                {% endif %}
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
    ) cp
    INNER JOIN {{ ref('curve_optimism_pools') }} ta
        ON cp.project_contract_address = ta.pool
        AND cp.bought_id = ta.tokenid
    INNER JOIN {{ ref('curve_optimism_pools') }} tb
        ON cp.project_contract_address = tb.pool
        AND cp.sold_id = tb.tokenid
)
, dexs_with_decimals AS (
    SELECT
        dexs.*
        , erc20_bought.decimals as token_bought_decimals
        , erc20_sold.decimals as token_sold_decimals
        -- Calculate curve used decimals based on pool type and token IDs
        , case
            when dexs.pool_type = 'meta' and dexs.bought_id = INT256 '0' then 18
            else coalesce(erc20_bought.decimals, 18)
        end as curve_decimals_bought
        , case
            when dexs.pool_type = 'meta' and dexs.bought_id = INT256 '0' then coalesce(erc20_bought.decimals, 18)
            else coalesce(erc20_sold.decimals, 18)
        end as curve_decimals_sold
    FROM dexs
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = dexs.token_bought_address
        AND erc20_bought.blockchain = 'optimism'
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = dexs.token_sold_address
        AND erc20_sold.blockchain = 'optimism'
)

SELECT DISTINCT
    'optimism' AS blockchain
    ,'curve' AS project
    ,'1' AS version
    ,CAST(date_trunc('DAY', dexs_with_decimals.block_time) AS date) AS block_date
    ,CAST(date_trunc('MONTH', dexs_with_decimals.block_time) AS date) AS block_month
    ,dexs_with_decimals.block_time
    ,dexs_with_decimals.block_number
    -- Adjust raw amounts so that generic enrichment (amount_raw / 10^token_decimals) yields correct token units
    ,CAST(
        dexs_with_decimals.token_bought_amount_raw * 
        power(10, dexs_with_decimals.token_bought_decimals - dexs_with_decimals.curve_decimals_bought)
        AS UINT256
    ) as token_bought_amount_raw
    ,CAST(
        dexs_with_decimals.token_sold_amount_raw * 
        power(10, dexs_with_decimals.token_sold_decimals - dexs_with_decimals.curve_decimals_sold)
        AS UINT256
    ) as token_sold_amount_raw
    ,dexs_with_decimals.token_bought_address
    ,dexs_with_decimals.token_sold_address
    ,dexs_with_decimals.taker
    ,dexs_with_decimals.maker
    ,dexs_with_decimals.project_contract_address
    ,dexs_with_decimals.tx_hash
    ,dexs_with_decimals.evt_index
FROM dexs_with_decimals