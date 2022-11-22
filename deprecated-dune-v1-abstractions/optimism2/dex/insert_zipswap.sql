CREATE OR REPLACE FUNCTION dex.insert_zipswap(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
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
	    SELECT 
	    t.evt_block_time AS block_time,
	    'Zipswap' AS project,
	    '1' AS version,
	    'DEX' AS category,
	    "to" AS trader_a,
	    NULL::bytea AS trader_b,
	    -- logic from ethereum/dex/trades/insert_uniswap_v2
	    CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
	    CASE WHEN "amount0In" = 0 OR "amount1Out" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw,
	    NULL::numeric AS usd_amount,
	    CASE WHEN "amount0Out" = 0 THEN token1 ELSE token0 END AS token_a_address,
	    CASE WHEN "amount0In" = 0 OR "amount1Out" = 0 THEN token1 ELSE token0 END AS token_b_address,
	    t.contract_address as exchange_contract_address,
	    t.evt_tx_hash AS tx_hash,
	    NULL::integer[] AS trace_address,
	    t.evt_index
	    FROM zipswap."UniswapV2Pair_evt_Swap" t
		 INNER JOIN zipswap."UniswapV2Factory_evt_PairCreated" f ON f.pair = t.contract_address

	WHERE t.evt_block_time >= start_ts AND t.evt_block_time < end_ts
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
SELECT dex.insert_zipswap(
    '2021-11-10',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now()
    AND project = 'Zipswap'
);
/*
INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT dex.insert_zipswap(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Zipswap'),
        now()
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
