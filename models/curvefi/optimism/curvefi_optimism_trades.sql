{{ config(
    
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

-- This should depend on 'curvefi_optimism_pools' running first
-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql
-- Start Time
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.StableSwap_evt_TokenExchange
-- UNION ALL
-- SELECT MIN(evt_block_time) FROM curvefi_optimism.MetaPoolSwap_evt_TokenExchange
{% set project_start_date = '2022-01-17' %}

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
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
                AND s.evt_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} s 
            WHERE t.evt_block_number = s.evt_block_number
                AND t.evt_tx_hash = s.evt_tx_hash
                AND t.evt_index = s.evt_index
                {% if is_incremental() %}
                AND s.evt_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
        )
        {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
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
                AND s.evt_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
        )
        {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    ) cp
    INNER JOIN {{ ref('curvefi_optimism_pools') }} ta
        ON cp.project_contract_address = ta.pool
        AND cp.bought_id = ta.tokenid
    INNER JOIN {{ ref('curvefi_optimism_pools') }} tb
        ON cp.project_contract_address = tb.pool
        AND cp.sold_id = tb.tokenid
    LEFT JOIN {{ source('tokens_optimism', 'erc20') }} ea
        ON ea.contract_address = ta.token
    LEFT JOIN {{ source('tokens_optimism', 'erc20') }} eb
        ON eb.contract_address = tb.token
)
SELECT DISTINCT
    'optimism' AS blockchain,
    'curve' AS project,
    '1' AS version,
    CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month,
    dexs.block_time,
    COALESCE(erc20a.symbol,p_bought.symbol)AS token_bought_symbol,
    COALESCE(erc20b.symbol,p_sold.symbol) AS token_sold_symbol
    ,case
        when lower(COALESCE(erc20a.symbol,p_bought.symbol)) > lower(COALESCE(erc20b.symbol,p_sold.symbol)) then concat(COALESCE(erc20b.symbol,p_sold.symbol), '-', COALESCE(erc20a.symbol,p_bought.symbol))
        else concat(COALESCE(erc20a.symbol,p_bought.symbol), '-', COALESCE(erc20b.symbol,p_sold.symbol))
    end as token_pair,
    --On Sell: Metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
    --On Buy: Metapools seem to always use the curve pool token's decimals (18) if bought_id = CAST(0 as INT256)
    dexs.token_bought_amount_raw / POWER(10 , (CASE WHEN pool_type = 'meta' AND bought_id = CAST(0 as INT256) THEN 18 ELSE COALESCE(erc20a.decimals,p_bought.decimals) END) ) AS token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10 , (CASE WHEN pool_type = 'meta' AND bought_id = CAST(0 as INT256) THEN COALESCE(erc20a.decimals,p_bought.decimals) ELSE COALESCE(erc20b.decimals,p_sold.decimals) END) )  AS token_sold_amount,
    dexs.token_bought_amount_raw  AS token_bought_amount_raw,
    dexs.token_sold_amount_raw  AS token_sold_amount_raw,
    coalesce(
	    --On Sell: Metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
	    --On Buy: Metapools seem to always use the curve pool token's decimals (18) if bought_id = CAST(0 as INT256)
        dexs.token_bought_amount_raw / POWER(10 , CASE WHEN pool_type = 'meta' AND bought_id = CAST(0 as INT256) THEN 18 ELSE COALESCE(erc20a.decimals,p_bought.decimals) END) * p_bought.price,
        dexs.token_sold_amount_raw / POWER(10 , CASE WHEN pool_type = 'meta' AND bought_id = CAST(0 as INT256) THEN COALESCE(erc20a.decimals,p_bought.decimals) ELSE COALESCE(erc20b.decimals,p_sold.decimals) END) * p_sold.price
    ) as amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx."from") as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" as tx_from,
    tx.to as tx_to,
    dexs.evt_index,
    dexs.pool_type
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    AND dexs.block_number = tx.block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

