CREATE TABLE IF NOT EXISTS zeroex.view_fills (
    "timestamp" timestamptz,
    protocol_version text,
    transaction_hash bytea,
    evt_index integer,
    maker_address bytea,
    taker_address bytea,
    maker_token bytea,
    maker_symbol text,
    maker_asset_filled_amount float,
    taker_token bytea,
    taker_symbol text,
    taker_asset_filled_amount float,
    fee_recipient_address bytea,
    volume_usd float,
    protocol_fee_paid_eth numeric
);


CREATE OR REPLACE FUNCTION zeroex.insert_fills(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH
    v3_fills AS (
        SELECT
            evt_block_time AS timestamp
            , 'v3' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills."makerAddress" AS maker_address
            , fills."takerAddress" AS taker_address
            , SUBSTRING(fills."makerAssetData",17,20) AS maker_token
            , mt.symbol AS maker_symbol
            , fills."makerAssetFilledAmount" / (10^mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills."takerAssetData",17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills."takerAssetFilledAmount" / (10^tt.decimals) AS taker_asset_filled_amount
            , fills."feeRecipientAddress" AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills."takerAssetFilledAmount" / 1e6) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills."makerAssetFilledAmount" / 1e6) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills."takerAssetFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills."makerAssetFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills."takerAssetFilledAmount" / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills."makerAssetFilledAmount" / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills."takerAssetFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills."makerAssetFilledAmount" / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills."takerAssetFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills."makerAssetFilledAmount" / 1e18) * mp.price
                    ELSE COALESCE((fills."makerAssetFilledAmount" / (10^mt.decimals))*mp.price,(fills."takerAssetFilledAmount" / (10^tt.decimals))*tp.price)
                END AS volume_usd
            , fills."protocolFeePaid" / 1e18 AS protocol_fee_paid_eth
        FROM zeroex_v3."Exchange_evt_Fill" fills
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills."takerAssetData",17,20) IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills."takerAssetData",17,20) IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE SUBSTRING(fills."takerAssetData",17,20)
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills."makerAssetData",17,20) IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills."makerAssetData",17,20) IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE SUBSTRING(fills."makerAssetData",17,20)
                END = mp.contract_address
        LEFT JOIN erc20.tokens mt ON mt.contract_address = SUBSTRING(fills."makerAssetData",17,20)
        LEFT JOIN erc20.tokens tt ON tt.contract_address = SUBSTRING(fills."takerAssetData",17,20)
        WHERE fills.evt_block_time >= start_ts
              AND fills.evt_block_time < end_ts
    )
    , v2_1_fills AS (
        SELECT
            evt_block_time AS timestamp
            , 'v2' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills."makerAddress" AS maker_address
            , fills."takerAddress" AS taker_address
            , SUBSTRING(fills."makerAssetData",17,20) AS maker_token
            , mt.symbol AS maker_symbol
            , fills."makerAssetFilledAmount" / (10^mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills."takerAssetData",17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills."takerAssetFilledAmount" / (10^tt.decimals) AS taker_asset_filled_amount
            , fills."feeRecipientAddress" AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills."takerAssetFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills."makerAssetFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills."takerAssetFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills."makerAssetFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills."takerAssetFilledAmount" / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills."makerAssetFilledAmount" / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills."takerAssetFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills."makerAssetFilledAmount" / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills."takerAssetFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills."makerAssetFilledAmount" / 1e18) * mp.price
                    ELSE COALESCE((fills."makerAssetFilledAmount" / (10^mt.decimals))*mp.price,(fills."takerAssetFilledAmount" / (10^tt.decimals))*tp.price)
                END AS volume_usd
            , NULL::NUMERIC AS protocol_fee_paid_eth
        FROM zeroex_v2."Exchange2.1_evt_Fill" fills
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills."takerAssetData",17,20) IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills."takerAssetData",17,20) IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE SUBSTRING(fills."takerAssetData",17,20)
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills."makerAssetData",17,20) IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills."makerAssetData",17,20) IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE SUBSTRING(fills."makerAssetData",17,20)
                END = mp.contract_address
        LEFT JOIN erc20.tokens mt ON mt.contract_address = SUBSTRING(fills."makerAssetData",17,20)
        LEFT JOIN erc20.tokens tt ON tt.contract_address = SUBSTRING(fills."takerAssetData",17,20)
        WHERE fills.evt_block_time >= start_ts
              AND fills.evt_block_time < end_ts
    )
    , v4_limit_fills AS (

        SELECT
            fills.evt_block_time AS timestamp
            , 'v4' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills."maker" AS maker_address
            , fills."taker" AS taker_address
            , fills."makerToken" AS maker_token
            , mt.symbol AS maker_symbol
            , fills."makerTokenFilledAmount" / (10^mt.decimals) AS maker_asset_filled_amount
            , fills."takerToken" AS taker_token
            , tt.symbol AS taker_symbol
            , fills."takerTokenFilledAmount" / (10^tt.decimals) AS taker_asset_filled_amount
            , fills."feeRecipient" AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills."takerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills."makerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills."takerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills."makerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills."takerTokenFilledAmount" / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills."makerTokenFilledAmount" / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                    ELSE COALESCE((fills."makerTokenFilledAmount" / (10^mt.decimals))*mp.price,(fills."takerTokenFilledAmount" / (10^tt.decimals))*tp.price)
                END AS volume_usd
            , fills."protocolFeePaid"/ 1e18 AS protocol_fee_paid_eth
        FROM zeroex."ExchangeProxy_evt_LimitOrderFilled" fills
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills."takerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN fills."takerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE fills."takerToken"
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills."makerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN fills."makerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                    ELSE fills."makerToken"
                END = mp.contract_address
        LEFT JOIN erc20.tokens mt ON mt.contract_address = fills."makerToken"
        LEFT JOIN erc20.tokens tt ON tt.contract_address = fills."takerToken"
        WHERE fills.evt_block_time >= start_ts
              AND fills.evt_block_time < end_ts
    )

    , v4_rfq_fills AS (
      SELECT
          fills.evt_block_time AS timestamp
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills."maker" AS maker_address
          , fills."taker" AS taker_address
          , fills."makerToken" AS maker_token
          , mt.symbol AS maker_symbol
          , fills."makerTokenFilledAmount" / (10^mt.decimals) AS maker_asset_filled_amount
          , fills."takerToken" AS taker_token
          , tt.symbol AS taker_symbol
          , fills."takerTokenFilledAmount" / (10^tt.decimals) AS taker_asset_filled_amount
          , NULL::BYTEA AS fee_recipient_address
          , CASE
                  WHEN tp.symbol = 'USDC' THEN (fills."takerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'USDC' THEN (fills."makerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'TUSD' THEN (fills."takerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'TUSD' THEN (fills."makerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'USDT' THEN (fills."takerTokenFilledAmount" / 1e6) * tp.price
                  WHEN mp.symbol = 'USDT' THEN (fills."makerTokenFilledAmount" / 1e6) * mp.price
                  WHEN tp.symbol = 'DAI' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                  WHEN mp.symbol = 'DAI' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                  WHEN tp.symbol = 'WETH' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                  WHEN mp.symbol = 'WETH' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                  ELSE COALESCE((fills."makerTokenFilledAmount" / (10^mt.decimals))*mp.price,(fills."takerTokenFilledAmount" / (10^tt.decimals))*tp.price)
              END AS volume_usd
          , NULL::NUMERIC AS protocol_fee_paid_eth
      FROM zeroex."ExchangeProxy_evt_RfqOrderFilled" fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills."takerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills."takerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                  ELSE fills."takerToken"
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills."makerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills."makerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                  ELSE fills."makerToken"
              END = mp.contract_address
      LEFT JOIN erc20.tokens mt ON mt.contract_address = fills."makerToken"
      LEFT JOIN erc20.tokens tt ON tt.contract_address = fills."takerToken"
      WHERE fills.evt_block_time >= start_ts
              AND fills.evt_block_time < end_ts
    ), otc_fills as
    (
      SELECT
          fills.evt_block_time AS timestamp
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills."maker" AS maker_address
          , fills."taker" AS taker_address
          , fills."makerToken" AS maker_token
          , mt.symbol AS maker_symbol
          , fills."makerTokenFilledAmount" / (10^mt.decimals) AS maker_asset_filled_amount
          , fills."takerToken" AS taker_token
          , tt.symbol AS taker_symbol
          , fills."takerTokenFilledAmount" / (10^tt.decimals) AS taker_asset_filled_amount
          , NULL::BYTEA AS fee_recipient_address
          , CASE
                  WHEN tp.symbol = 'USDC' THEN (fills."takerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'USDC' THEN (fills."makerTokenFilledAmount" / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'TUSD' THEN (fills."takerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'TUSD' THEN (fills."makerTokenFilledAmount" / 1e18) --don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'USDT' THEN (fills."takerTokenFilledAmount" / 1e6) * tp.price
                  WHEN mp.symbol = 'USDT' THEN (fills."makerTokenFilledAmount" / 1e6) * mp.price
                  WHEN tp.symbol = 'DAI' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                  WHEN mp.symbol = 'DAI' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                  WHEN tp.symbol = 'WETH' THEN (fills."takerTokenFilledAmount" / 1e18) * tp.price
                  WHEN mp.symbol = 'WETH' THEN (fills."makerTokenFilledAmount" / 1e18) * mp.price
                  ELSE COALESCE((fills."makerTokenFilledAmount" / (10^mt.decimals))*mp.price,(fills."takerTokenFilledAmount" / (10^tt.decimals))*tp.price)
              END AS volume_usd
          , NULL::NUMERIC AS protocol_fee_paid_eth
      FROM zeroex."ExchangeProxy_evt_OtcOrderFilled" fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills."takerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills."takerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                  ELSE fills."takerToken"
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills."makerToken" IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills."makerToken" IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
                  ELSE fills."makerToken"
              END = mp.contract_address
      LEFT JOIN erc20.tokens mt ON mt.contract_address = fills."makerToken"
      LEFT JOIN erc20.tokens tt ON tt.contract_address = fills."takerToken"
      WHERE fills.evt_block_time >= start_ts
              AND fills.evt_block_time < end_ts

    ),

    all_fills as (
    
    SELECT * FROM v3_fills

    UNION ALL

    SELECT * FROM v2_1_fills

    UNION ALL

    SELECT * FROM v4_limit_fills

    UNION ALL

    SELECT * FROM v4_rfq_fills

    UNION ALL
    
    SELECT * FROM otc_fills
    ), rows AS (
            INSERT INTO zeroex.view_fills (
                "timestamp",
                protocol_version,
                transaction_hash,
                evt_index,
                maker_address,
                taker_address,
                maker_token,
                maker_symbol,
                maker_asset_filled_amount,
                taker_token,
                taker_symbol,
                taker_asset_filled_amount,
                fee_recipient_address,
                volume_usd,
                protocol_fee_paid_eth
            )
            SELECT
                "timestamp",
                protocol_version,
                transaction_hash,
                evt_index,
                maker_address,
                taker_address,
                maker_token,
                maker_symbol,
                maker_asset_filled_amount,
                taker_token,
                taker_symbol,
                taker_asset_filled_amount,
                fee_recipient_address,
                volume_usd,
                protocol_fee_paid_eth
            FROM all_fills
            ON CONFLICT DO NOTHING
            RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;

CREATE UNIQUE INDEX IF NOT EXISTS zeroex_fills_unique ON zeroex.view_fills (transaction_hash, evt_index);
CREATE INDEX IF NOT EXISTS zeroex_fills_time_index ON zeroex.view_fills (timestamp);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT zeroex.insert_fills((SELECT max("timestamp") - interval '2 days' FROM zeroex.view_fills), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
