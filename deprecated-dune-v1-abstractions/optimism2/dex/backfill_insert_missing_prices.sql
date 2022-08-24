-- Since we're pulling prices from dex trades, we:
-- 1. Add in dex.trades
-- 2. Calculate token prices from those dex.trades
-- But these most recent trades may not have a usd amount if we haven't yet calculated a dex price from previous trades. So, we:
-- 3. Backfill the usd_amount of the most recent dex.trades, in order to fill this gap

CREATE OR REPLACE FUNCTION dex.backfill_insert_missing_prices(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
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
      block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / 10 ^ pa.decimals * pa.median_price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.median_price
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
        
        FROM dex.trades dexs
        
        
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
    
    WHERE dexs.block_time >= start_ts AND dexs.block_time < end_ts
    -- don't overwrite bridge tokens this way
    AND NOT EXISTS (SELECT 1 FROM prices.hourly_bridge_token_price_ratios WHERE token_a_address = bridge_token OR token_b_address = bridge_token)
    
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


/*
INSERT INTO cron.job (schedule, command)
VALUES ('18,48 * * * *', $$
    SELECT dex.backfill_insert_missing_prices(
        (SELECT max(block_time) - interval '25 hours' FROM dex.trades), --small 1 hr buffer for safety with time offsets
        now()
        )
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
