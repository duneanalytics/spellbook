{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "curvefi",
                                \'["msilb7"]\') }}'
    )
}}

-- This should depend on 'curvefi_optimism_pools' running first

-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql

SELECT DISTINCT
    'optimism' AS blockchain,
    'curve' AS project,
    1 AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    --metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
    token_a_amount_raw / POWER(10 , (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE COALESCE(erc20a.decimals,pa.decimals) END) ) AS token_bought_amount,
    token_b_amount_raw / POWER(10 , (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE COALESCE(erc20b.decimals,pb.decimals) END) )  AS token_sold_amount,
    token_a_amount_raw AS token_bought_amount_raw,
    token_b_amount_raw AS token_sold_amount_raw,
    coalesce(
        usd_amount,
	--metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
        token_a_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE COALESCE(erc20a.decimals,pa.decimals) END) * pa.price,
        token_b_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE COALESCE(erc20b.decimals,pb.decimals) END) * pb.price
    ) as usd_amount,

    token_a_address AS token_bought_address,
    token_b_address AS token_sold_address,
    coalesce(trader_a, tx."from") as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    trader_b AS maker,
    exchange_contract_address AS project_contract_address,
    tx_hash,
    tx."from" as tx_from,
    tx."to" as tx_to,
    trace_address,
    evt_index
    -- row_number() OVER (PARTITION BY project, tx_hash, dexs.evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
    -- Stableswap
     SELECT
        'stable' AS pool_type, -- has implications for decimals for curve
        t.evt_block_time AS block_time,
        t.buyer AS trader_a,
        NULL AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        "tokens_bought" AS token_a_amount_raw,
        "tokens_sold" AS token_b_amount_raw,
        NULL AS usd_amount,
        ta.token AS token_a_address,
        tb.token AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        CAST(NULL AS ARRAY<INT>) AS trace_address,
        t.evt_index, bought_id, sold_id,
        NULL AS underlying_decimals --used for metaswaps
    FROM {{ source('curvefi_optimism', 'StableSwap_evt_TokenExchange') }} t
        INNER JOIN {{ ref('curvefi_optimism_pools') }} ta
            ON t.contract_address = ta.pool
            AND t.bought_id = ta.tokenid
        INNER JOIN {{ ref('curvefi_optimism_pools') }} tb
            ON t.contract_address = tb.pool
            AND t.sold_id = tb.tokenid
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    
    UNION ALL
    -- MetaPoolSwap
    SELECT
    'meta' AS pool_type, -- has implications for decimals for curve
        t.evt_block_time AS block_time,
        t.buyer AS trader_a,
        NULL AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        "tokens_bought" AS token_a_amount_raw,
        "tokens_sold" AS token_b_amount_raw,
        NULL AS usd_amount,
        ta.token AS token_a_address,
        tb.token AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        CAST(NULL AS ARRAY<INT>) AS trace_address,
        t.evt_index, bought_id, sold_id,
        CASE WHEN bought_id = 0 THEN COALESCE(ea.decimals,pa.decimals) ELSE COALESCE(eb.decimals,pb.decimals) END AS underlying_decimals --used if meta
    FROM {{ source('curvefi_optimism', 'MetaPoolSwap_evt_TokenExchangeUnderlying') }} t
        INNER JOIN {{ ref('curvefi_optimism_pools') }} ta
            ON t.contract_address = ta.pool
            AND t.bought_id = ta.tokenid
        INNER JOIN {{ ref('curvefi_optimism_pools') }} tb
            ON t.contract_address = tb.pool
            AND t.sold_id = tb.tokenid
        LEFT JOIN {{ ref('tokens_optimism_erc20') }} ea ON ea.contract_address = ta.token
        LEFT JOIN {{ ref('tokens_optimism_erc20') }} eb ON eb.contract_address = tb.token
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    
    UNION ALL
    --BasicPoolSwap --to replace with decoded contract eventually
    --example https://optimistic.etherscan.io/address/0x3da3153e26a230d918bb9f9428a8d60349b73379#events
    SELECT
    'basic' as pool_type,
    block_time,
    substring(topic2,25,40) AS trader_a,
    NULL AS trader_b,
    ta.token as token_a_amount_raw, --2nd bought
    tb.token as token_b_amount_raw, --1st sold
    NULL AS usd_amount,
    bytea2numeric_v2(substring(data,3+64*2,64))::int as token_a_address, --2nd bought
    bytea2numeric_v2(substring(data,3+64*2,64))::int as token_b_address, --2nd bought
    contract_address AS exchange_contract_address,
    CAST(NULL AS ARRAY<INT>) AS trace_address,
    bytea2numeric_v2(substring(data,3,64))::int as tokena,--1st sold
    bytea2numeric_v2(substring(data,3+64*1,64)) as tokena_amount, --1st sold
    DENSE_RANK() (PARTITION BY tx_index ORDER BY index) AS evt_index,
    bytea2numeric_v2(substring(data,3+64*3,64))::int AS bought_id,
    bytea2numeric_v2(substring(data,3+64*1,64))::int AS sold_id

    FROM {{ source('ethereum', 'logs') }} l
        INNER JOIN {{ ref('curvefi_optimism_pools') }} ta
            ON t.contract_address = ta.pool
            AND bytea2numeric_v2(substring(data,3+64*3,64))::int = ta.tokenid --t.bought_id = ta.tokenid
            AND ta.version = 'Basic Pool'
        INNER JOIN {{ ref('curvefi_optimism_pools') }} tb
            ON t.contract_address = tb.pool
            AND bytea2numeric_v2(substring(data,3+64*1,64))::int = tb.tokenid --t.sold_id = tb.tokenid
            AND tb.version = 'Basic Pool'
    WHERE topic1 = '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140'
    {% if is_incremental() %}
        AND l.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    
    ) dexs
    INNER JOIN {{ source('ethereum', 'transactions') }} tx
        ON dexs.tx_hash = tx.hash
        AND dexs.evt_block_number = tx.block_number
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN {{ ref('tokens_optimism_erc20') }} erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN {{ source('prices', 'usd') }} pa
      ON pa.hour = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        {% if is_incremental() %}
        AND pa.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source('prices', 'usd') }} pb
      ON pb.hour = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        {% if is_incremental() %}
        AND pb.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}