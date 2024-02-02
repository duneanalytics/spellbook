{{
    config(
        schema = 'curvefi_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- This should depend on 'curvefi_optimism_pools' running first
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
    INNER JOIN {{ ref('curvefi_optimism_pools') }} ta
        ON cp.project_contract_address = ta.pool
        AND cp.bought_id = ta.tokenid
    INNER JOIN {{ ref('curvefi_optimism_pools') }} tb
        ON cp.project_contract_address = tb.pool
        AND cp.sold_id = tb.tokenid
)
SELECT DISTINCT
    'optimism' AS blockchain
    ,'curve' AS project
    ,'1' AS version
    ,CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month
    ,dexs.block_time
    ,dexs.block_number
    ,dexs.token_bought_amount_raw
    ,dexs.token_sold_amount_raw
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,dexs.taker
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,dexs.evt_index

    --unique to curve in dex lineage, pull extra columns to calculate amount / amount_usd downstream in enrichment phase
    ,dexs.pool_type
    ,dexs.bought_id
FROM dexs
