CREATE TABLE IF NOT EXISTS zeroex.view_0x_api_fills (
    tx_hash bytea,
    evt_index integer,
    contract_address bytea,
    block_time timestamptz,
    maker bytea,
    taker bytea,
    taker_token bytea,
    maker_token bytea,
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
WITH zeroex_tx_raw AS (
            SELECT DISTINCT
                v3.evt_tx_hash AS tx_hash
                , case when "takerAddress" = '\x63305728359c088a52b0b0eeec235db4d31a67fc'::BYTEA then "takerAddress"
                       else null
                end as affiliate_address
            FROM zeroex_v3."Exchange_evt_Fill" v3
            WHERE
                -- nuo
                v3."takerAddress" = '\x63305728359c088a52b0b0eeec235db4d31a67fc'::BYTEA
                OR
                -- contains a bridge order
                (v3."feeRecipientAddress" = '\x1000000000000000000000000000000000000011'::BYTEA
                    AND SUBSTRING(v3."makerAssetData",1,4) = '\xdc1600f3'::bytea)
            AND evt_block_time >= start_ts AND evt_block_time < end_ts

            UNION

            SELECT
                tx_hash
                , affiliate_address as affiliate_address
            from zeroex."view_api_affiliate_data"
            WHERE block_time >= start_ts AND block_time < end_ts
        ),
        zeroex_tx AS (
                    SELECT
                        tx_hash
                        , MAX(affiliate_address::text)::bytea as affiliate_address
                    from zeroex_tx_raw
                    GROUP BY 1
        ),
        v3_fills_no_bridge AS (
              SELECT fills.evt_tx_hash AS tx_hash
                  , fills.evt_index
                  , fills.contract_address
                  , evt_block_time AS block_time
                  , fills."makerAddress" AS maker
                  , fills."takerAddress" AS taker
                  , SUBSTRING(fills."takerAssetData",17,20) AS taker_token
                  , SUBSTRING(fills."makerAssetData",17,20) AS maker_token
                  , fills."takerAssetFilledAmount"  AS taker_token_amount_raw
                  , fills."makerAssetFilledAmount"  AS maker_token_amount_raw
                  , 'Native Fill v3' as type
                  , COALESCE(zeroex_tx.affiliate_address, fills."feeRecipientAddress") as affiliate_address
                  , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
                  , (fills."feeRecipientAddress" = '\x86003b044f70dac0abc80ac8957305b6370893ed'::bytea) AS matcha_limit_order_flag
              FROM zeroex_v3."Exchange_evt_Fill" fills
              LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.evt_tx_hash
              WHERE
                  (SUBSTRING("makerAssetData",1,4) != '\xdc1600f3'::BYTEA)
                  AND (
                      zeroex_tx.tx_hash IS NOT NULL
                      OR fills."feeRecipientAddress" = '\x86003b044f70dac0abc80ac8957305b6370893ed'::bytea
                  )
                  AND evt_block_time >= start_ts AND evt_block_time < end_ts
          ),
          v4_rfq_fills_no_bridge AS (
              SELECT fills.evt_tx_hash AS tx_hash
                  , fills.evt_index
                  , fills.contract_address
                  , fills.evt_block_time AS block_time
                  , fills.maker AS maker
                  , fills.taker AS taker
                  , fills."takerToken" AS taker_token
                  , fills."makerToken" AS maker_token
                  , fills."takerTokenFilledAmount"  AS taker_token_amount_raw
                  , fills."makerTokenFilledAmount"  AS maker_token_amount_raw
                  , 'Native Fill v4' as type
                  , zeroex_tx.affiliate_address as affiliate_address
                  , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
                  , FALSE AS matcha_limit_order_flag
              FROM zeroex."ExchangeProxy_evt_RfqOrderFilled" fills
              LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.evt_tx_hash
              AND evt_block_time >= start_ts AND evt_block_time < end_ts
        ),
        v4_limit_fills_no_bridge AS (
              SELECT fills.evt_tx_hash AS tx_hash
                  , fills.evt_index
                  , fills.contract_address
                  , fills.evt_block_time AS block_time
                  , fills.maker AS maker
                  , fills.taker AS taker
                  , fills."takerToken" AS taker_token
                  , fills."makerToken" AS maker_token
                  , fills."takerTokenFilledAmount"  AS taker_token_amount_raw
                  , fills."makerTokenFilledAmount"  AS maker_token_amount_raw
                  , 'Native Fill v4' as type
                  , COALESCE(zeroex_tx.affiliate_address, fills."feeRecipient") as affiliate_address
                  , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
                  , (fills."feeRecipient" = '\x86003b044f70dac0abc80ac8957305b6370893ed'::bytea) AS matcha_limit_order_flag
              FROM zeroex."ExchangeProxy_evt_LimitOrderFilled" fills
              LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.evt_tx_hash
              AND evt_block_time >= start_ts AND evt_block_time < end_ts
      ),
      otc_fills AS (
              SELECT fills.evt_tx_hash AS tx_hash
                  , fills.evt_index
                  , fills.contract_address
                  , fills.evt_block_time AS block_time
                  , fills.maker AS maker
                  , fills.taker AS taker
                  , fills."takerToken" AS taker_token
                  , fills."makerToken" AS maker_token
                  , fills."takerTokenFilledAmount"  AS taker_token_amount_raw
                  , fills."makerTokenFilledAmount"  AS maker_token_amount_raw
                  , 'Native Fill v4' as type
                  , zeroex_tx.affiliate_address as affiliate_address
                  , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
                  , FALSE AS matcha_limit_order_flag
              FROM zeroex."ExchangeProxy_evt_OtcOrderFilled" fills
              LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.evt_tx_hash
              AND evt_block_time >= start_ts AND evt_block_time < end_ts
      ),
      -- bridge fills
    	ERC20BridgeTransfer AS (
    		SELECT 	logs.tx_hash,
    				INDEX AS evt_index,
            logs.contract_address,
    				block_time AS block_time,
            substring(DATA,141,20) AS maker,
            substring(DATA,173,20) AS taker,
    				substring(DATA,13,20) AS taker_token,
    				substring(DATA,45,20) AS maker_token,
    				bytea2numericpy(substring(DATA,77,20)) AS taker_token_amount_raw,
    				bytea2numericpy(substring(DATA,109,20)) AS maker_token_amount_raw,
            'Bridge Fill' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
     		FROM ethereum."logs" logs
        join zeroex_tx on zeroex_tx.tx_hash = logs.tx_hash
    		WHERE topic1 = '\x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'::bytea
                AND block_time >= start_ts AND block_time < end_ts
        ),
      BridgeFill AS (
    		SELECT 	logs.tx_hash,
    				INDEX AS evt_index,
            logs.contract_address,
    				block_time AS block_time,
            substring(DATA,13,20) AS maker,
            '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::bytea AS taker,
    				substring(DATA,45,20) AS taker_token,
    				substring(DATA,77,20) AS maker_token,
    				bytea2numericpy(substring(DATA,109,20)) AS taker_token_amount_raw,
    				bytea2numericpy(substring(DATA,141,20)) AS maker_token_amount_raw,
            'Bridge Fill' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
     		FROM ethereum."logs" logs
        join zeroex_tx on zeroex_tx.tx_hash = logs.tx_hash
    		WHERE topic1 = '\xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9'::bytea
                AND block_time >= start_ts AND block_time < end_ts
    	),

      NewBridgeFill AS (
        SELECT  logs.tx_hash,
            INDEX AS evt_index,
            logs.contract_address,
            block_time AS block_time,
            substring(DATA,13,20) AS maker,
            '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::bytea AS taker,
            substring(DATA,45,20) AS taker_token,
            substring(DATA,77,20) AS maker_token,
            bytea2numeric(substring(DATA,109,20)) AS taker_token_amount_raw,
            bytea2numeric(substring(DATA,141,20)) AS maker_token_amount_raw,
            'Bridge Fill' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
        FROM ethereum."logs" logs
        join zeroex_tx on zeroex_tx.tx_hash = logs.tx_hash
        WHERE topic1 = '\xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8'::bytea
                and contract_address = '\x22f9dcf4647084d6c31b2765f6910cd85c178c18'::bytea
                AND block_time >= start_ts AND block_time < end_ts
      ),
      direct_PLP AS (
        SELECT 	plp.evt_tx_hash,
    				plp.evt_index AS evt_index,
            plp.contract_address,
    				plp.evt_block_time AS block_time,
            provider AS maker,
            recipient AS taker,
    				"inputToken" AS taker_token,
    				"outputToken" AS maker_token,
    				"inputTokenAmount" AS taker_token_amount_raw,
    				"outputTokenAmount" AS maker_token_amount_raw,
            'PLP Direct' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
     		FROM zeroex."ExchangeProxy_evt_LiquidityProviderSwap" plp
        join zeroex_tx on zeroex_tx.tx_hash = plp.evt_tx_hash
              WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
    	),

    	direct_uniswapv2 AS (
    		SELECT 	swap.evt_tx_hash AS tx_hash,
    				swap.evt_index,
            swap.contract_address,
    				swap.evt_block_time AS block_time,
    				swap.contract_address AS maker,
    				LAST_VALUE(swap."to") OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
    				CASE
    					WHEN swap."amount0In" > swap."amount1In" THEN pair.token0
    					ELSE pair.token1
    					END AS taker_token,
    				CASE
    					WHEN swap."amount0In" > swap."amount1In" THEN pair.token1
    					ELSE pair.token0
    					END AS maker_token,
            CASE
    					WHEN swap."amount0In" > swap."amount1In" THEN swap."amount0In"
    					ELSE swap."amount1In"
    					END AS taker_token_amount_raw,
            CASE
              WHEN swap."amount0In" > swap."amount1In" THEN swap."amount1Out"
              ELSE swap."amount0Out"
              END AS maker_token_amount_raw,
    			 	'UniswapV2 Direct' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
    		FROM uniswap_v2."Pair_evt_Swap" swap
    		LEFT JOIN uniswap_v2."Factory_evt_PairCreated" pair ON pair.pair = swap.contract_address
        join zeroex_tx on zeroex_tx.tx_hash = swap.evt_tx_hash
    		WHERE sender = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
                AND swap.evt_block_time >= start_ts AND swap.evt_block_time < end_ts
    	),

    	direct_sushiswap AS (
    		SELECT 	swap.evt_tx_hash AS tx_hash,
    				swap.evt_index,
            swap.contract_address,
    				swap.evt_block_time AS block_time,
    				swap.contract_address AS maker,
    				LAST_VALUE(swap."to") OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
    				CASE
    					WHEN swap."amount0In" > swap."amount1In" THEN pair.token0
    					ELSE pair.token1
    					END AS taker_token,
    				CASE
    					WHEN swap."amount0In" > swap."amount1In" THEN pair.token1
    					ELSE pair.token0
    					END AS maker_token,
            CASE
              WHEN swap."amount0In" > swap."amount1In" THEN swap."amount0In"
              ELSE swap."amount1In"
              END AS taker_token_amount_raw,
            CASE
              WHEN swap."amount0In" > swap."amount1In" THEN swap."amount1Out"
              ELSE swap."amount0Out"
              END AS maker_token_amount_raw,
    				'Sushiswap Direct' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
    		FROM sushi."Pair_evt_Swap" swap
    		LEFT JOIN sushi."Factory_evt_PairCreated" pair ON pair.pair = swap.contract_address
        join zeroex_tx on zeroex_tx.tx_hash = swap.evt_tx_hash
    		WHERE sender = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
                AND swap.evt_block_time >= start_ts AND swap.evt_block_time < end_ts
    	),

      direct_uniswapv3 AS (
    		SELECT 	swap.evt_tx_hash AS tx_hash,
    				swap.evt_index,
            swap.contract_address,
    				swap.evt_block_time AS block_time,
    				swap.contract_address AS maker,
    				LAST_VALUE(swap."recipient") OVER (PARTITION BY swap.evt_tx_hash ORDER BY swap.evt_index) AS taker,
    				pair.token1 AS taker_token,
    				pair.token0 AS maker_token,
            abs(swap."amount1") AS taker_token_amount_raw,
            abs(swap."amount0") AS maker_token_amount_raw,
    			 	'UniswapV3 Direct' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
    		FROM uniswap_v3."Pair_evt_Swap" swap
    		LEFT JOIN uniswap_v3."Factory_evt_PoolCreated" pair ON pair.pool = swap.contract_address
        join zeroex_tx on zeroex_tx.tx_hash = swap.evt_tx_hash
    		WHERE sender = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
                AND swap.evt_block_time >= start_ts AND swap.evt_block_time < end_ts
    	),

    	all_tx AS (
          SELECT * FROM direct_uniswapv2
          UNION ALL
          SELECT * FROM direct_uniswapv3
          UNION ALL
          SELECT * FROM direct_sushiswap
          UNION ALL
          SELECT * FROM direct_PLP
          UNION ALL
          SELECT * FROM ERC20BridgeTransfer
          UNION ALL
          SELECT * FROM BridgeFill
          UNION ALL
          SELECT * FROM NewBridgeFill
          UNION ALL
          SELECT * FROM v3_fills_no_bridge
          UNION ALL
          SELECT * FROM v4_rfq_fills_no_bridge
          UNION ALL
          SELECT * FROM v4_limit_fills_no_bridge
          UNION ALL
          SELECT * FROM otc_fills
    	),
    	total_volume AS (
    		SELECT 	all_tx.tx_hash,
      				all_tx.evt_index,
              all_tx.contract_address,
      				all_tx.block_time,
      				maker,
      				case when taker = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::bytea then tx."from" else taker end as taker, -- fix the user masked by ProxyContract issue
      				taker_token,
      				maker_token,
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
      					WHEN tp.symbol = 'USDT' THEN (all_tx.taker_token_amount_raw / 1e6) * tp.price
      					WHEN mp.symbol = 'USDT' THEN (all_tx.maker_token_amount_raw / 1e6) * mp.price
      					WHEN tp.symbol = 'DAI' THEN (all_tx.taker_token_amount_raw / 1e18) * tp.price
      					WHEN mp.symbol = 'DAI' THEN (all_tx.maker_token_amount_raw / 1e18) * mp.price
      					WHEN tp.symbol = 'WETH' THEN (all_tx.taker_token_amount_raw / 1e18) * tp.price
      					WHEN mp.symbol = 'WETH' THEN (all_tx.maker_token_amount_raw / 1e18) * mp.price
      					ELSE COALESCE((all_tx.maker_token_amount_raw / (10^mt.decimals))*mp.price,(all_tx.taker_token_amount_raw / (10^tt.decimals))*tp.price)
      					END AS volume_usd
      		FROM all_tx
          INNER JOIN ethereum.transactions tx
                                  ON all_tx.tx_hash = tx.hash
                                    AND tx.block_time >= start_ts
                                    AND tx.block_time < end_ts
                                    -- AND tx.block_number >= start_block
                                    -- AND tx.block_number < end_block
      		LEFT JOIN prices.usd tp ON date_trunc('minute', all_tx.block_time) = tp.minute
      								AND all_tx.taker_token = tp.contract_address
                                                                AND tp.minute >= start_ts
                                                                AND tp.minute < end_ts
      		LEFT JOIN prices.usd mp ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
      								AND all_tx.maker_token = mp.contract_address
                                                                AND mp.minute >= start_ts
                                                                AND mp.minute < end_ts
      		LEFT JOIN erc20.tokens mt ON mt.contract_address = all_tx.maker_token
      		LEFT JOIN erc20.tokens tt ON tt.contract_address = all_tx.taker_token
          -- WHERE all_tx.block_time >= start_ts AND all_tx.block_time < end_ts
      ),
        rows AS (
            INSERT INTO zeroex.view_0x_api_fills (
                tx_hash,
                evt_index,
                contract_address,
                block_time,
                maker,
                taker,
                taker_token,
                maker_token,
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
                taker_token,
                maker_token,
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
            ON CONFLICT DO NOTHING
            RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;
      
CREATE UNIQUE INDEX IF NOT EXISTS zeroex_api_fills_unique ON zeroex.view_0x_api_fills (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS zeroex_api_fills_time_index ON zeroex.view_0x_api_fills (block_time);

--backfill
SELECT zeroex.insert_0x_api_fills('2020-01-29', (SELECT now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM zeroex.view_0x_api_fills LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT zeroex.insert_0x_api_fills((SELECT max(block_time) - interval '2 days' FROM zeroex.view_0x_api_fills), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
