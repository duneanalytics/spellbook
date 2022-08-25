CREATE TABLE IF NOT EXISTS zeroex.view_0x_api_fills (
    tx_hash bytea,
    evt_index integer,
    contract_address bytea,
    block_time timestamptz,
    maker bytea,
    taker bytea,
    taker_token_address bytea,
    taker_token_symbol text,
    maker_token_address bytea,
    maker_token_symbol text,
    taker_token_amount float,
    taker_token_amount_raw numeric,
    maker_token_amount float,
    maker_token_amount_raw numeric,
    "type" text,
    affiliate_address bytea,
    swap_flag boolean,
    matcha_limit_order_flag boolean,
    volume_usd float
);

CREATE OR REPLACE FUNCTION zeroex.insert_0x_api_fills (start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH zeroex_tx AS (
          SELECT
              tx_hash
              , MAX(affiliate_address::text)::bytea as affiliate_address
          from zeroex."view_api_affiliate_data"
          WHERE block_time >= start_ts AND block_time < end_ts
          GROUP BY 1
        ),
        bridge_fill AS (
          SELECT
            logs.tx_hash,
            INDEX AS evt_index,
            logs.contract_address,
            block_time AS block_time,
            substring(DATA,13,20) AS maker,
            '\xdef1abe32c034e558cdd535791643c58a13acc10'::bytea AS taker,
            substring(DATA,45,20) AS taker_token,
            substring(DATA,77,20) AS maker_token,
            bytea2numeric(substring(DATA,109,20)) AS taker_token_amount_raw,
            bytea2numeric(substring(DATA,141,20)) AS maker_token_amount_raw,
            'Bridge Fill' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
        FROM optimism."logs" logs
        join zeroex_tx on zeroex_tx.tx_hash = logs.tx_hash
        WHERE topic1 = '\xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8'::bytea
                and contract_address = '\xa3128d9b7cca7d5af29780a56abeec12b05a6740'::bytea
                AND block_time >= start_ts AND block_time < end_ts
      ),
    	total_volume AS (
        SELECT
            all_tx.tx_hash,
            all_tx.evt_index,
            all_tx.contract_address,
            all_tx.block_time,
            maker,
            case when taker = '\xdef1abe32c034e558cdd535791643c58a13acc10'::bytea then tx."from" else taker end as taker, -- fix the user masked by ProxyContract issue
            taker_token as taker_token_address,
            tt.symbol as taker_token_symbol,
            maker_token as maker_token_address,
            mt.symbol as maker_token_symbol,
            taker_token_amount_raw / (10^tt.decimals) AS taker_token_amount,
            taker_token_amount_raw,
            maker_token_amount_raw / (10^mt.decimals) AS maker_token_amount,
            maker_token_amount_raw,
            all_tx.type,
            affiliate_address,
            swap_flag,
            matcha_limit_order_flag,
      			CASE
      				WHEN tp.symbol = 'USDC' THEN (all_tx.taker_token_amount_raw / 1e6)--don't multiply by anything as these assets are USD
      				WHEN mp.symbol = 'USDC' THEN (all_tx.maker_token_amount_raw / 1e6)--don't multiply by anything as these assets are USD
      				WHEN tp.symbol = 'TUSD' THEN (all_tx.taker_token_amount_raw / 1e18)--don't multiply by anything as these assets are USD
      				WHEN mp.symbol = 'TUSD' THEN (all_tx.maker_token_amount_raw / 1e18)--don't multiply by anything as these assets are USD
      				WHEN tp.symbol = 'USDT' THEN (all_tx.taker_token_amount_raw / 1e6) * tp.median_price
      				WHEN mp.symbol = 'USDT' THEN (all_tx.maker_token_amount_raw / 1e6) * mp.median_price
      				WHEN tp.symbol = 'DAI' THEN (all_tx.taker_token_amount_raw / 1e18) * tp.median_price
      				WHEN mp.symbol = 'DAI' THEN (all_tx.maker_token_amount_raw / 1e18) * mp.median_price
      				WHEN tp.symbol = 'WETH' THEN (all_tx.taker_token_amount_raw / 1e18) * tp.median_price
      				WHEN mp.symbol = 'WETH' THEN (all_tx.maker_token_amount_raw / 1e18) * mp.median_price
      				ELSE COALESCE((all_tx.maker_token_amount_raw / (10^mt.decimals))*mp.median_price,(all_tx.taker_token_amount_raw / (10^tt.decimals))*tp.median_price)
      				END AS volume_usd
  		FROM bridge_fill all_tx
  		INNER JOIN optimism.transactions tx
              ON all_tx.tx_hash = tx.hash
                AND tx.block_time >= start_ts
                AND tx.block_time < end_ts
      	LEFT JOIN prices.approx_prices_from_dex_data tp
      	        ON date_trunc('hour', all_tx.block_time) = tp.hour
  					AND all_tx.taker_token = tp.contract_address
                    AND tp.hour >= start_ts
                    AND tp.hour < end_ts
  		LEFT JOIN prices.approx_prices_from_dex_data mp
  		        ON DATE_TRUNC('hour', all_tx.block_time) = mp.hour
  				    AND all_tx.maker_token = mp.contract_address
                    AND mp.hour >= start_ts
                    AND mp.hour < end_ts
  		LEFT JOIN erc20.tokens mt ON mt.contract_address = all_tx.maker_token
  		LEFT JOIN erc20.tokens tt ON tt.contract_address = all_tx.taker_token
      ),
        rows AS (
            INSERT INTO zeroex.view_0x_api_fills (
                tx_hash,
                evt_index,
                contract_address,
                block_time,
                maker,
                taker,
                taker_token_address,
                taker_token_symbol,
                maker_token_address,
                maker_token_symbol,
                taker_token_amount,
                taker_token_amount_raw,
                maker_token_amount,
                maker_token_amount_raw,
                "type",
                affiliate_address,
                swap_flag,
                matcha_limit_order_flag,
                volume_usd
            )
            SELECT
                tx_hash,
                evt_index,
                contract_address,
                block_time,
                maker,
                taker,
                taker_token_address,
                taker_token_symbol,
                maker_token_address,
                maker_token_symbol,
                taker_token_amount,
                taker_token_amount_raw,
                maker_token_amount,
                maker_token_amount_raw,
                "type",
                affiliate_address,
                swap_flag,
                matcha_limit_order_flag,
                volume_usd
            FROM total_volume
            ON CONFLICT (tx_hash, evt_index)
            DO UPDATE SET
                volume_usd = EXCLUDED.volume_usd,
                taker_token_symbol = EXCLUDED.taker_token_symbol,
                maker_token_symbol = EXCLUDED.maker_token_symbol,
                taker_token_amount = EXCLUDED.taker_token_amount,
                maker_token_amount = EXCLUDED.maker_token_amount
            RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;

CREATE UNIQUE INDEX IF NOT EXISTS zeroex_api_fills_unique ON zeroex.view_0x_api_fills (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS zeroex_api_fills_time_index ON zeroex.view_0x_api_fills (block_time);

--backfill
SELECT zeroex.insert_0x_api_fills('2021-12-28', (SELECT now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM zeroex.view_0x_api_fills LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT zeroex.insert_0x_api_fills((SELECT max(block_time) - interval '2 days' FROM zeroex.view_0x_api_fills), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
