CREATE OR REPLACE FUNCTION dex.insert_zeroex(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
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
      -- 0x api
      SELECT
          block_time,
          '0x API' AS project,
          NULL AS version,
          'Aggregator' AS category,
          "taker" AS trader_a,
          "maker" AS trader_b,
          "taker_token_amount_raw" AS token_a_amount_raw,
          "maker_token_amount_raw" AS token_b_amount_raw,
          NULL::numeric AS usd_amount,
          taker_token_address AS token_a_address,
          maker_token_address AS token_b_address,
          contract_address AS exchange_contract_address,
          tx_hash,
          NULL::integer[] AS trace_address,
          evt_index
      FROM zeroex."view_0x_api_fills"
      where swap_flag is TRUE

      UNION ALL

      -- Matcha
      SELECT
          block_time,
          'Matcha' AS project,
          NULL AS version,
          'Aggregator' AS category,
          "taker" AS trader_a,
          "maker" AS trader_b,
          "taker_token_amount_raw" AS token_a_amount_raw,
          "maker_token_amount_raw" AS token_b_amount_raw,
          NULL::numeric AS usd_amount,
          taker_token_address AS token_a_address,
          maker_token_address AS token_b_address,
          contract_address AS exchange_contract_address,
          tx_hash,
          NULL::integer[] AS trace_address,
          evt_index
      FROM zeroex."view_0x_api_fills"
      where affiliate_address ='\x86003b044f70dac0abc80ac8957305b6370893ed'

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

/*
-- fill 2021, 0x only launched after op2
SELECT dex.insert_zeroex(
    '2021-12-28',
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-12-28'
    AND block_time <= now() - interval '20 minutes'
    AND project IN ('0x API', 'Matcha')
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT dex.insert_zeroex(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project IN ('0x API', 'Matcha')),
        now()
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
