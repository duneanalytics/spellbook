CREATE TABLE IF NOT EXISTS zerion.trades (
    block_time TIMESTAMP
    , trader BYTEA
    , usd_volume DECIMAL
    , protocol TEXT
    , tx_hash BYTEA
    , sold_token_amount DECIMAL
    , bought_token_amount DECIMAL
    , sold_token_address BYTEA
    , bought_token_address BYTEA
    , sold_token_symbol TEXT
    , bought_token_symbol TEXT
);

CREATE OR REPLACE FUNCTION zerion.trades (start_ts timestamptz, end_ts timestamptz=NOW()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

  WITH hashflow_event_decoding AS (
      SELECT tx_hash
      , SUBSTRING(data, 13, 20) AS trader
      , SUBSTRING(data,33,32) AS tx_id
      , SUBSTRING(data, 109, 20) AS sold_token_address
      , SUBSTRING(data, 77, 20) AS bought_token_address
      , bytea2numericpy(SUBSTRING(data, 173, 20)) AS sold_token_amount
      , bytea2numericpy(SUBSTRING(data, 141, 20)) AS bought_token_amount
      FROM ethereum.logs
      WHERE topic1 ='\x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e'
      AND block_time >= start_ts AND block_time < end_ts
          UNION ALL
      SELECT tx_hash
      , SUBSTRING(data, 45, 20) AS trader
      , SUBSTRING(data,65,32) AS tx_id
      , SUBSTRING(data, 141, 20) AS sold_token
      , SUBSTRING(data, 109, 20) AS bought_token
      , bytea2numericpy(SUBSTRING(data, 205, 20)) AS sold_token_amount
      , bytea2numericpy(SUBSTRING(data, 173, 20)) AS bought_token_amount
      FROM ethereum.logs         
      WHERE topic1 ='\xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5'
      AND block_time >= start_ts AND block_time < end_ts
      )
      
  , hashflow_trades AS (
      SELECT t.block_time
      , tx."from" AS trader
      , CASE WHEN l.tx_hash IS NOT NULL THEN COALESCE(bought_token_amount/power(10, tp.decimals) * tp.price, sold_token_amount/power(10, mp.decimals) * mp.price) 
          ELSE NULL END AS usd_volume
      , 'Hashflow' AS protocol
      , t.tx_hash
      , CASE WHEN l.tx_hash is NOT NULL THEN sold_token_amount/power(10,mp.decimals) 
          ELSE NULL END AS sold_token_amount
      , CASE WHEN l.tx_hash IS NOT NULL THEN bought_token_amount/power(10,tp.decimals) 
          ELSE NULL END AS bought_token_amount
      , sold_token_address
      , bought_token_address
      , CASE WHEN substring(input, 113, 20) = '\x0000000000000000000000000000000000000000'::bytea THEN 'ETH' 
          ELSE mp.symbol END AS sold_token_symbol
      , CASE WHEN substring(input, 81, 20) = '\x0000000000000000000000000000000000000000'::bytea THEN 'ETH' 
          ELSE tp.symbol END AS bought_token_symbol
      FROM ethereum.traces t
      LEFT JOIN ethereum.transactions tx ON tx.hash = t.tx_hash
      AND tx.block_time >= start_ts
      AND tx.block_time < end_ts
      LEFT JOIN hashflow_event_decoding l ON l.tx_id = substring(t.input,325,32) 
      JOIN prices.usd tp ON tp.minute = date_trunc('minute', t.block_time) 
          AND tp.contract_address = CASE WHEN substring(input, 81, 20) = '\x0000000000000000000000000000000000000000'::bytea 
              THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea ELSE substring(input, 81, 20) END
      JOIN prices.usd mp ON mp.minute = date_trunc('minute', t.block_time) 
          AND mp.contract_address = CASE WHEN substring(input, 113, 20) = '\x0000000000000000000000000000000000000000'::bytea
              THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea else substring(input, 113, 20) END
      WHERE t."to" IN ('\xa18607ca4a3804cc3cd5730eafefcc47a7641643')
      AND t.block_time >= start_ts AND t.block_time < end_ts
      AND substring(input,1,4) IN ('\xba93c39c') -- swap()
      AND substring(input, 325, 1) ='\x01' -- Zerion label
      AND t.success
      )

  , uniswap_and_forks AS (
      SELECT dt.block_time
      , dt.tx_from AS trader
      , dt.usd_amount AS usd_volume
      , dt.project AS protocol
      , dt.tx_hash
      , token_a_amount AS sold_token_amount
      , token_b_amount AS bought_token_amount
      , token_a_address AS sold_token_address
      , token_b_address AS bought_token_address
      , token_a_symbol AS sold_token_symbol
      , token_b_symbol AS bought_token_symbol
      FROM dex.trades dt
      INNER JOIN ethereum.traces et ON et.tx_hash=dt.tx_hash
      AND et.block_time >= start_ts
      AND et.block_time < end_ts
      AND et.to IN ( 
          '\xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F', -- Sushi
          '\x7a250d5630B4cF539739dF2C5dAcb4c659F2488D', -- Uni 
          '\xE592427A0AEce92De3Edee1F18E0157C05861564' -- Uni v3
          )
      AND position('\x7a6572696f6e' IN et.input::bytea) > 260
      WHERE dt.block_time >= start_ts AND dt.block_time < end_ts
      LIMIT 100
      )

  , oneinch_after_20_09_2021 AS (
      SELECT dt.block_time
      , dt.tx_from AS trader
      , COALESCE(dt.usd_amount
          , token_a_amount*pua.median_price
          , token_b_amount*pub.median_price
          ) AS usd_volume
      , project AS protocol
      , dt.tx_hash
      , token_a_amount AS sold_token_amount
      , token_b_amount AS bought_token_amount
      , token_a_address AS sold_token_address
      , token_b_address AS bought_token_address
      , token_a_symbol AS sold_token_symbol
      , token_b_symbol AS bought_token_symbol
      FROM ethereum."traces" et
      INNER JOIN dex.trades dt ON et.tx_hash=dt.tx_hash AND dt.project='1inch'
      AND dt.block_time >= start_ts
      AND dt.block_time < end_ts
      LEFT JOIN prices."prices_from_dex_data" pua ON dt.token_a_address=pua.contract_address AND date_trunc('hour', dt.block_time)=pua.hour
      LEFT JOIN prices."prices_from_dex_data" pub ON  dt.token_b_address=pub.contract_address AND date_trunc('hour', dt.block_time)=pub.hour
      WHERE "to" IN ('\x11111112542d85b3ef69ae05771c2dccff4faa26', '\x1111111254fb6c44bac0bed2854e76f90643097d')
      AND ENCODE(SUBSTRING("input" FROM 1 FOR 4), 'hex') IN (
                  'b0431182',--clipperSwap
                  '9994dd15',--clipperSwapTo
                  'd6a92a5d',--clipperSwapToWithPermit
                  '7c025200',--swap
                  'e449022e',--uniswapV3Swap
                  'bc80f1a8',--uniswapV3SwapTo
                  '2521b930',--uniswapV3SwapToWithPermit
                  '2e95b6c8',--unoswap
                  'a1251d75',--unoswapWithPermit,
                  'd0a3b665',-- fillOrderRFQ,
                  'baba5855',-- fillOrderRFQTo
                  '4cc4a27b' -- fillOrderRFQToWithPermit
          )
      AND REVERSE( SUBSTRING( REVERSE( ENCODE("input", 'hex') ) FROM 1 FOR 8 ) ) = '51d40aca' --zerion marker
      AND et.block_time >= start_ts AND et.block_time < end_ts
      )

  , zrx_api_volume AS (
      SELECT block_time
      , taker AS trader
      , COALESCE(volume_usd
          , maker_token_amount*puds.median_price
          , taker_token_amount*pudb.median_price
          ) AS usd_volume
      , '0x API' AS protocol
      , tx_hash
      , maker_token_amount AS sold_token_amount
      , taker_token_amount AS bought_token_amount
      , maker_token AS sold_token_address
      , taker_token AS bought_token_address
      , tb.symbol AS bought_token_symbol
      , ts.symbol AS sold_token_symbol
      FROM zeroex.view_0x_api_fills f
      LEFT JOIN prices."prices_from_dex_data" puds ON f.maker_token=puds.contract_address AND date_trunc('hour', f.block_time)=puds.hour
      LEFT JOIN prices."prices_from_dex_data" pudb ON f.taker_token=pudb.contract_address AND date_trunc('hour', f.block_time)=pudb.hour
      LEFT JOIN erc20."tokens" tb ON f.maker_token=tb.contract_address
      LEFT JOIN erc20."tokens" ts ON f.taker_token=ts.contract_address
      WHERE affiliate_address= '\x7CBa0Eb7A94068324583BE7771C5ECDa25e4C4d1'
      AND swap_flag is true
      AND f.block_time >= start_ts AND f.block_time < end_ts
      )

  , paraswap_trades AS (
      SELECT datetime AS block_time
      , trader AS trader
      , COALESCE(volume
          , sold_token_amount*pua.median_price
          , bought_token_amount*pub.median_price
          ) AS usd_volume
      , 'Paraswap' AS protocol
      , hash AS tx_hash
      , sold_token_amount
      , bought_token_amount
      , "srcToken" AS sold_token_address
      , "destToken" AS bought_token_address
      , bought_token_symbol
      , sold_token_symbol
      FROM (
          SELECT tx.block_time AS datetime
          , tx."from" AS trader
          , swap."srcToken"
          , swap."destToken"
          , swap."srcAmount"/POWER(10, ts.decimals) AS sold_token_amount
          , swap."receivedAmount"/POWER(10, tb.decimals) AS bought_token_amount
          , tb.symbol AS bought_token_symbol
          , ts.symbol AS sold_token_symbol
          , tx.hash
          , GREATEST(
          swap."srcAmount" * (
              CASE
                      WHEN swap."srcToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 
                          (
                              SELECT p.price
                              FROM prices."layer1_usd" p
                              WHERE p.symbol = 'ETH' 
                              AND p.minute = date_trunc('minute', evt_block_time)
                              LIMIT 1
                          )
                      ELSE
                          (
                              SELECT p.price
                              FROM prices."usd" p
                              WHERE p.contract_address = swap."srcToken"
                              AND p.minute = date_trunc('minute', evt_block_time)
                              LIMIT 1
                          )
                  END
              ) / POWER(10, (
                      SELECT decimals FROM (
                          (SELECT decimals FROM erc20.tokens WHERE contract_address = swap."srcToken" LIMIT 1)
                          UNION ALL
                          SELECT '18'
                      ) _dec LIMIT 1
                  )),
          swap."receivedAmount" * (
              CASE
                      WHEN swap."destToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 
                          (
                              SELECT p.price
                              FROM prices."layer1_usd" p
                              WHERE p.symbol = 'ETH' 
                              AND p.minute = date_trunc('minute', evt_block_time)
                              LIMIT 1
                          )
                      ELSE
                          (
                              SELECT p.price
                              FROM prices."usd" p
                              WHERE p.contract_address = swap."destToken"
                              AND p.minute = date_trunc('minute', evt_block_time)
                              LIMIT 1
                          )
                  END
              ) / POWER(10, (
                      SELECT decimals FROM (
                          (SELECT decimals FROM erc20.tokens WHERE contract_address = swap."destToken" LIMIT 1)
                          UNION ALL
                          SELECT '18'
                      ) _dec LIMIT 1
                  ))
          ) AS volume
      FROM (
              SELECT evt_tx_hash
              , evt_block_time
              , "srcToken"
              , "destToken"
              , "srcAmount"
              , "receivedAmount"
              FROM paraswap."AugustusSwapper5.0_evt_Swapped"
              WHERE referrer = 'zerion'
              AND evt_block_time >= start_ts AND evt_block_time < end_ts
          UNION ALL
              SELECT evt_tx_hash
              , evt_block_time
              , "srcToken"
              , "destToken"
              , "srcAmount"
              , "receivedAmount"
              FROM paraswap."AugustusSwapper5.0_evt_Bought"
              WHERE referrer = 'zerion'
              AND evt_block_time >= start_ts AND evt_block_time < end_ts
          UNION ALL
              SELECT call_tx_hash AS evt_tx_hash
              , call_block_time AS evt_block_time
              , path[1] AS "srcToken"
              , path[array_length(path, 1)] AS "destToken"
              , "amountIn" AS "srcAmount"
              , "amountOutMin" AS "receivedAmount"
              FROM paraswap."AugustusSwapper5.0_call_swapOnUniswap"
              WHERE referrer = 6
              AND call_success = true
              AND call_block_time >= start_ts AND call_block_time < end_ts
          UNION ALL
              SELECT call_tx_hash AS evt_tx_hash
              , call_block_time AS evt_block_time
              , path[1] AS "srcToken"
              , path[array_length(path, 1)] AS "destToken"
              , "amountIn" AS "srcAmount"
              , "amountOutMin" AS "receivedAmount"
              FROM paraswap."AugustusSwapper5.0_call_swapOnUniswapFork"
              WHERE referrer = 6
              AND call_success = true
              AND call_block_time >= start_ts AND call_block_time < end_ts
      ) swap
      LEFT JOIN ethereum.transactions tx ON tx.hash = swap.evt_tx_hash
        AND block_time >= start_ts
        AND block_time < end_ts
      LEFT JOIN erc20."tokens" tb ON swap."destToken"=tb.contract_address
      LEFT JOIN erc20."tokens" ts ON swap."srcToken"=ts.contract_address
      ) zerion_through_paraswap
      LEFT JOIN prices."prices_from_dex_data" pua ON zerion_through_paraswap."srcToken"=pua.contract_address AND date_trunc('hour', zerion_through_paraswap.datetime)=pua.hour
      LEFT JOIN prices."prices_from_dex_data" pub ON zerion_through_paraswap."destToken"=pub.contract_address AND date_trunc('hour', zerion_through_paraswap.datetime)=pub.hour
  )

  , defi_sdk_txs AS (
      SELECT tx."hash" AS hash
      , substring(traces.input, 81, 20) as sender
      , tx."value" as value
      , tx."block_time" as block_time
      FROM ethereum."logs" 
      INNER JOIN ethereum."transactions" tx ON tx.hash = tx_hash
          AND tx.success = TRUE 
          AND tx.block_time >= start_ts
          AND tx.block_time < end_ts
      RIGHT JOIN ethereum."traces" traces ON traces.tx_hash = tx.hash
          AND traces.to = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
          AND traces.from = '\xb2be281e8b11b47fec825973fc8bb95332022a54'
          AND traces.block_time >= start_ts
          AND traces.block_time < end_ts
      WHERE topic1 = '\x5c416a271db2ac40f70515df028f580eeb1e2f7be2e656664553b83d9e15a039' 
          AND contract_address = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
          AND substring(traces.input, 1, 4) = '\x695f7219'
          AND tx.hash <> '\x84552aca155ee0b6c7f444cc94f3161791afa7980ddf6b364bc8155f01cf5623'
          AND logs.block_time >= start_ts AND logs.block_time < end_ts
      GROUP by tx.hash, tx.value, tx.block_time, sender
      )

  , outgoing_token_transafers_with_prices AS (
      SELECT hash
      , sender
      , defi_sdk_txs."value" as value
      , block_time
      , tokens.symbol as symbol
      , transfers.contract_address as token_address
      , transfers.value/POWER(10, CASE tokens.decimals IS NULL WHEN true THEN 18 else tokens.decimals END) as volume
      , (transfers.value/POWER(10, CASE tokens.decimals IS NULL WHEN true THEN 18 else tokens.decimals END))*p.price as usd_volume
      FROM defi_sdk_txs
      INNER JOIN erc20."ERC20_evt_Transfer" transfers 
          ON transfers.evt_tx_hash = defi_sdk_txs.hash
          AND transfers.to = sender
      INNER JOIN prices.usd p 
          ON p.minute = date_trunc('minute', transfers.evt_block_time)
          AND transfers.contract_address = p.contract_address
      LEFT JOIN erc20."tokens" tokens 
          ON transfers.contract_address = tokens.contract_address
      WHERE transfers.from = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
      )

  , outgoing_eth_transfers_with_prices AS (
      SELECT hash
      , sender
      , defi_sdk_txs."value" as value
      , defi_sdk_txs."block_time" as block_time
      , 'ETH' as symbol
      , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::BYTEA as token_address
      , traces.value/POWER(10, 18) as volume
      , (traces.value/POWER(10, 18))*p.price as usd_volume
      FROM defi_sdk_txs
      INNER JOIN ethereum."traces" traces 
          ON traces.tx_hash = hash
          AND traces.to = sender
          AND traces.block_time >= start_ts
          AND traces.block_time < end_ts
      INNER JOIN prices.usd p 
          ON p.minute = date_trunc('minute', defi_sdk_txs.block_time)
          AND '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' = p.contract_address
      )

  , outgoing_tx_hashes AS (
      SELECT distinct hash FROM (
          SELECT hash FROM outgoing_eth_transfers_with_prices 
          UNION 
          SELECT hash FROM outgoing_token_transafers_with_prices
      ) unique_outgoing_txs
      )

  , incoming_token_transafers_with_prices AS (
      SELECT hash
      , sender
      , defi_sdk_txs."value" as value
      , block_time
      , tokens.symbol as symbol
      , transfers.contract_address as token_address
      , transfers.value/POWER(10, CASE tokens.decimals IS NULL WHEN true THEN 18 else tokens.decimals END) as volume
      , (transfers.value/POWER(10, CASE tokens.decimals IS NULL WHEN true THEN 18 else tokens.decimals END))*p.price as usd_volume
      FROM defi_sdk_txs
      INNER JOIN erc20."ERC20_evt_Transfer" transfers 
          ON transfers.evt_tx_hash = defi_sdk_txs.hash
          AND transfers.from = sender
      INNER JOIN prices.usd p 
          ON p.minute = date_trunc('minute', transfers.evt_block_time)
          AND transfers.contract_address = p.contract_address
      LEFT JOIN erc20."tokens" tokens 
          ON transfers.contract_address = tokens.contract_address
      WHERE transfers.to = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
      )

  , incoming_eth_transfers_with_prices AS (
      SELECT hash
      , sender
      , defi_sdk_txs."value" as value
      , defi_sdk_txs."block_time" as block_time
      , 'ETH' as symbol
      , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::BYTEA as token_address
      , traces.value/POWER(10, 18) as volume
      , (traces.value/POWER(10, 18))*p.price as usd_volume
      FROM defi_sdk_txs
      INNER JOIN ethereum."traces" traces 
          ON defi_sdk_txs.hash = traces.tx_hash
          AND traces.to = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
          AND traces.from = '\xb2be281e8b11b47fec825973fc8bb95332022a54' 
          AND traces.value > 0
          AND traces.block_time >= start_ts
          AND traces.block_time < end_ts
      INNER JOIN prices.usd p 
          ON p.minute = date_trunc('minute', defi_sdk_txs.block_time)
          AND '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' = p.contract_address
      )

  , incoming_tx_hashes AS (
      SELECT distinct hash FROM (
          SELECT hash FROM incoming_eth_transfers_with_prices 
          UNION 
          SELECT hash FROM incoming_token_transafers_with_prices
      ) unique_incoming_txs
      )

  , incoming_transfers_with_prices AS (
      SELECT * FROM incoming_eth_transfers_with_prices -- WHERE hash NOT IN (SELECT hash FROM outgoing_tx_hashes)
      UNION 
      SELECT * FROM incoming_token_transafers_with_prices -- WHERE hash NOT IN (SELECT hash FROM outgoing_tx_hashes)
      )

  , outgoing_transfers_with_prices AS (
      SELECT * FROM outgoing_eth_transfers_with_prices WHERE hash NOT IN (SELECT hash FROM incoming_tx_hashes)
      UNION 
      SELECT * FROM outgoing_token_transafers_with_prices WHERE hash NOT IN (SELECT hash FROM incoming_tx_hashes)
      )

  , transfers_with_prices AS (
      SELECT * FROM incoming_transfers_with_prices
      UNION
      SELECT * FROM outgoing_transfers_with_prices
      )

  , defi_sdk_trades AS (
      SELECT block_time
      , sender AS trader
      , usd_volume
      , 'DeFi SDK' AS protocol
      , hash AS tx_hash
      , NULL::decimal AS sold_token_amount
      , NULL::decimal AS bought_token_amount
      , NULL::bytea AS sold_token_address
      , NULL::bytea AS bought_token_address
      , NULL::text AS sold_token_symbol
      , NULL::text AS bought_token_symbol
      FROM transfers_with_prices
      )

  , rows AS (
    INSERT INTO zerion.trades (
      block_time
      , trader
      , usd_volume
      , protocol
      , tx_hash
      , sold_token_amount
      , bought_token_amount
      , sold_token_address
      , bought_token_address
      , sold_token_symbol
      , bought_token_symbol
    )
    SELECT block_time
    , trader
    , usd_volume
    , protocol
    , tx_hash
    , sold_token_amount
    , bought_token_amount
    , sold_token_address
    , bought_token_address
    , sold_token_symbol
    , bought_token_symbol
    FROM (
      SELECT * FROM hashflow_trades
      UNION ALL
      SELECT * FROM uniswap_and_forks
      UNION ALL
      SELECT * FROM oneinch_after_20_09_2021
      UNION ALL
      SELECT * FROM zrx_api_volume
      UNION ALL
      SELECT * FROM paraswap_trades
      UNION ALL
      SELECT * FROM defi_sdk_trades
      ) zerion_all_trades
      ON CONFLICT DO NOTHING
      RETURNING 1
  )
  SELECT count(*) INTO r from rows;
  RETURN r;
  END
  $function$
;

CREATE INDEX IF NOT EXISTS zerion_trades_time_index ON zerion.trades USING btree (block_time);
CREATE UNIQUE INDEX IF NOT EXISTS zerion_trades_unique ON zerion.trades USING btree (tx_hash, protocol, sold_token_address);

--backfill
SELECT zerion.trades('2019-01-01', '2020-01-01') --WHERE NOT EXISTS (SELECT * FROM zerion.trades LIMIT 1);

--backfill
SELECT zerion.trades('2020-01-01', '2021-01-01') --WHERE NOT EXISTS (SELECT * FROM zerion.trades LIMIT 1);

--backfill
SELECT zerion.trades('2021-01-01', '2022-01-01') --WHERE NOT EXISTS (SELECT * FROM zerion.trades LIMIT 1);

--backfill
SELECT zerion.trades('2022-01-01', (SELECT NOW() - interval '20 minutes')) --WHERE NOT EXISTS (SELECT * FROM zerion.trades LIMIT 1);


INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT zerion.trades((SELECT MAX(block_time) - interval '2 days' FROM zerion.trades), (SELECT NOW() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;