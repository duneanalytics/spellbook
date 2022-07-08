CREATE OR REPLACE FUNCTION dex.insert_curve(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )
WITH curve_pools AS (
    WITH base_pools AS (
        SELECT "arg0" AS tokenid, output_0 AS token, contract_address AS pool
            FROM curvefi."StableSwap_call_coins" WHERE call_success
	    GROUP BY 1,2,3 --unique
        )
    , meta_pools AS (
    SELECT tokenid, token, et."contract_address" AS pool
    FROM (
        SELECT mp.evt_tx_hash, bp.tokenid + 1 AS tokenid, bp.token
        FROM curvefi."PoolFactory_evt_MetaPoolDeployed" mp
        INNER JOIN base_pools bp
            ON mp.base_pool = bp.pool
	GROUP BY 1,2,3 --unique
	    
        UNION ALL
        SELECT mp.evt_tx_hash, 0 AS tokenid, mp."coin" AS token
        FROM curvefi."PoolFactory_evt_MetaPoolDeployed" mp
        INNER JOIN base_pools bp
            ON mp.base_pool = bp.pool
	GROUP BY 1,2,3 --unique
	    
        ) mps
        -- the exchange address appears as an erc20 minted to itself (not in the deploymeny event)
        INNER JOIN erc20."ERC20_evt_Transfer" et
            ON et.evt_tx_hash = mps.evt_tx_hash
            AND et."from" = '\x0000000000000000000000000000000000000000'
            AND et."to" = et."contract_address"
            
        GROUP BY 1,2,3
    )
    SELECT * FROM (
        SELECT tokenid, token, pool FROM base_pools
        UNION ALL
        SELECT tokenid, token, pool FROM meta_pools
        ) a
	GROUP BY 1,2,3 --unique
    ORDER BY pool ASC, tokenid ASC
)
SELECT DISTINCT
    dexs.block_time,
    erc20a.symbol AS token_a_symbol,
    erc20b.symbol AS token_b_symbol,
    --metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
    token_a_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE erc20a.decimals END) AS token_a_amount,
    token_b_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE erc20b.decimals END)  AS token_b_amount,
    project,
    version,
    category,
    coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    coalesce(
        usd_amount,
	--metapools seem to always use the added coin's decimals if it's the one that's bought - even if the other token has less decimals (i.e. USDC)
        token_a_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE erc20a.decimals END) * pa.median_price,
        token_b_amount_raw / 10 ^ (CASE WHEN pool_type = 'meta' AND bought_id = 0 THEN underlying_decimals ELSE erc20b.decimals END) * pb.median_price
    ) as usd_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    tx."from" as tx_from,
    tx."to" as tx_to,
    trace_address,
    evt_index,
    row_number() OVER (PARTITION BY project, tx_hash, dexs.evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
    -- Stableswap
     SELECT
        'stable' AS pool_type, -- has implications for decimals for curve
        t.evt_block_time AS block_time,
        'Curve' AS project,
        '1' AS version,
        'DEX' AS category,
        t.buyer AS trader_a,
        NULL::bytea AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        "tokens_bought" AS token_a_amount_raw,
        "tokens_sold" AS token_b_amount_raw,
        NULL::numeric AS usd_amount,
        ta.token AS token_a_address,
        tb.token AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index, bought_id, sold_id,
        NULL::numeric AS underlying_decimals --used for metasaps
    FROM curvefi."StableSwap_evt_TokenExchange" t
        INNER JOIN curve_pools ta
            ON t.contract_address = ta.pool
            AND t.bought_id = ta.tokenid
        INNER JOIN curve_pools tb
            ON t.contract_address = tb.pool
            AND t.sold_id = tb.tokenid
    
    UNION ALL
    -- MetaPoolSwap
    SELECT
    'meta' AS pool_type, -- has implications for decimals for curve
        t.evt_block_time AS block_time,
        'Curve' AS project,
        '1' AS version,
        'DEX' AS category,
        t.buyer AS trader_a,
        NULL::bytea AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        "tokens_bought" AS token_a_amount_raw,
        "tokens_sold" AS token_b_amount_raw,
        NULL::numeric AS usd_amount,
        ta.token AS token_a_address,
        tb.token AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index, bought_id, sold_id,
        CASE WHEN bought_id = 0 THEN ea.decimals ELSE eb.decimals END AS underlying_decimals --used if meta
    FROM curvefi."MetaPoolSwap_evt_TokenExchangeUnderlying" t
        INNER JOIN curve_pools ta
            ON t.contract_address = ta.pool
            AND t.bought_id = ta.tokenid
        INNER JOIN curve_pools tb
            ON t.contract_address = tb.pool
            AND t.sold_id = tb.tokenid
        LEFT JOIN erc20.tokens ea ON ea.contract_address = ta.token
        LEFT JOIN erc20.tokens eb ON eb.contract_address = tb.token
    
    ) dexs
    INNER JOIN optimism.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts

    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.approx_prices_from_dex_data pa
      ON pa.hour = date_trunc('hour', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.hour >= start_ts
        AND pa.hour < end_ts
    LEFT JOIN prices.approx_prices_from_dex_data pb
      ON pb.hour = date_trunc('hour', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.hour >= start_ts
        AND pb.hour < end_ts


    -- update if we have new info on prices or the erc20
    ON CONFLICT (project, tx_hash, evt_index, trade_id)
    DO UPDATE SET
        usd_amount = EXCLUDED.usd_amount,
        token_a_amount = EXCLUDED.token_a_amount,
        token_b_amount = EXCLUDED.token_b_amount,
        token_a_symbol = EXCLUDED.token_a_symbol,
        token_b_symbol = EXCLUDED.token_b_symbol
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2021 (post-regenesis 11-11)
SELECT dex.insert_curve(
    '2021-11-10',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now()
    AND project = 'Curve'
);
/*
INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT dex.insert_curve(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Curve'),
        now()
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
