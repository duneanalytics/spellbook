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
        SELECT "arg0" AS tokenid, output_0 AS token, contract_address AS pool,
            MAX("arg0") OVER (PARTITION BY contract_address) AS max_token_id
            FROM curvefi."StableSwap_call_coins" WHERE call_success
        )
    , meta_pools AS (
        SELECT bp.tokenid, bp.token, mp."contract_address" AS pool -- start the pool
        FROM curvefi."PoolFactory_evt_MetaPoolDeployed" mp
        INNER JOIN base_pools bp
            ON mp.base_pool = bp.pool
        UNION ALL
        SELECT bp.max_token_id + 1 AS tokenid, mp."coin" AS token, mp."contract_address" AS pool
        FROM curvefi."PoolFactory_evt_MetaPoolDeployed" mp
        INNER JOIN base_pools bp
            ON mp.base_pool = bp.pool
        GROUP BY 1,2,3
    )
    SELECT * FROM (
        SELECT tokenid, token, pool FROM base_pools
        UNION ALL
        SELECT tokenid, token, pool FROM meta_pools
        ) a
    ORDER BY pool ASC, tokenid ASC
)
SELECT
    dexs.block_time,
    erc20a.symbol AS token_a_symbol,
    erc20b.symbol AS token_b_symbol,
    token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
    token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
    project,
    version,
    category,
    coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    coalesce(
        usd_amount,
        token_a_amount_raw / 10 ^ erc20a.decimals * pa.median_price,
        token_b_amount_raw / 10 ^ erc20b.decimals * pb.median_price
    ) as usd_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    tx."from" as tx_from,
    tx."to" as tx_to,
    trace_address,
    evt_index,
    row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
    --Stableswap
     SELECT
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
        t.evt_index
    FROM curvefi."StableSwap_evt_TokenExchange" t
        INNER JOIN curve_pools ta
            ON t.contract_address = ta.pool
            AND t.bought_id = ta.tokenid
        INNER JOIN curve_pools tb
            ON t.contract_address = tb.pool
            AND t.sold_id = tb.tokenid
    
    -- UNION ALL
    --MetaPoolSwap
    -- SELECT
    --     t.evt_block_time AS block_time,
    --     'Curve' AS project,
    --     '1' AS version,
    --     'DEX' AS category,
    --     t.buyer AS trader_a,
    --     NULL::bytea AS trader_b,
    --     -- when amount0 is negative it means trader_a is buying token0 from the pool
    --     "tokens_bought" AS token_a_amount_raw,
    --     "tokens_sold" AS token_b_amount_raw,
    --     NULL::numeric AS usd_amount,
    --     ta.token AS token_a_address,
    --     tb.token AS token_b_address,
    --     t.contract_address as exchange_contract_address,
    --     t.evt_tx_hash AS tx_hash,
    --     NULL::integer[] AS trace_address,
    --     t.evt_index
    -- FROM /*Exchange Underlying*/ t
    --     INNER JOIN curve_pools ta
    --         ON t.contract_address = ta.pool
    --         AND t.bought_id = ta.tokenid
    --     INNER JOIN curve_pools tb
    --         ON t.contract_address = tb.pool
    --         AND t.sold_id = tb.tokenid
    
    ) dexs
    INNER JOIN optimism.transactions tx
        ON dexs.tx_hash = tx.hash
        -- AND tx.block_time >= start_ts
        -- AND tx.block_time < end_ts

    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.approx_prices_from_dex_data pa
      ON pa.hour = date_trunc('hour', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        -- AND pa.hour >= start_ts
        -- AND pa.hour < end_ts
    LEFT JOIN prices.approx_prices_from_dex_data pb
      ON pb.hour = date_trunc('hour', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        -- AND pb.hour >= start_ts
        -- AND pb.hour < end_ts

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
    AND block_time <= now() - interval '20 minutes'
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
