BEGIN;
DROP MATERIALIZED VIEW IF EXISTS zeroex.view_fills;
CREATE MATERIALIZED VIEW zeroex.view_fills AS (
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
    )
    SELECT * FROM v3_fills

    UNION ALL

    SELECT * FROM v2_1_fills
);

CREATE UNIQUE INDEX IF NOT EXISTS zeroex_fills_unique ON zeroex.view_fills (transaction_hash, evt_index);
CREATE INDEX IF NOT EXISTS zeroex_fills_time_index ON zeroex.view_fills (timestamp);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY zeroex.view_fills')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
