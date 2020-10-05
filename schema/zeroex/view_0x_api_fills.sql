BEGIN;
DROP MATERIALIZED VIEW IF EXISTS zeroex.view_0x_api_fills;


CREATE OR REPLACE FUNCTION dune_user_generated.hex_to_numeric(str text) RETURNS numeric LANGUAGE PLPGSQL IMMUTABLE STRICT AS $$
declare
    i int;
    n int = length(str)/ 8;
    res dec = 0;
begin
    str := lpad($1, (n+ 1)* 8, '0');
    for i in 0..n loop
        if i > 0 then
            res:= res * 4294967296;
        end if;
        res:= res + concat('x', substr(str, i* 8+ 1, 8))::bit(32)::bigint::dec;
    end loop;
    return res;
end $$;

CREATE MATERIALIZED VIEW zeroex.view_0x_api_fills AS (

  WITH
  	ERC20BridgeTransfer AS (
  		SELECT 	tx_hash,
  				INDEX AS evt_index,
  				block_time AS TIMESTAMP,
  				substring(DATA,13,20) AS taker_token,
  				substring(DATA,45,20) AS maker_token,
  				dune_user_generated.HEX_TO_NUMERIC(right(substring(DATA,77,20)::varchar,-2)) AS taker_token_amount_raw,
  				dune_user_generated.HEX_TO_NUMERIC(right(substring(DATA,109,20)::varchar,-2)) AS maker_token_amount_raw,
  				substring(DATA,173,20) AS taker,
  				substring(DATA,141,20) AS maker
  		FROM ethereum."logs"
  		WHERE topic1 = '\x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'::bytea
  	),

  	bridge_fills AS (
  		SELECT 	logs.tx_hash,
  				logs.evt_index,
  				logs."timestamp",
  				logs.maker,
  				logs.taker,
  				logs.taker_token,
  				logs.maker_token,
  				logs.taker_token_amount_raw / (10^tt.decimals) AS taker_token_amount,
  				logs.maker_token_amount_raw / (10^mt.decimals) AS maker_token_amount,
  				'Bridge Fill' as type,
  				CASE
  					WHEN tp.symbol = 'USDC' THEN (logs.taker_token_amount_raw / 1e6)--don't multiply by anything as these assets are USD
  					WHEN mp.symbol = 'USDC' THEN (logs.maker_token_amount_raw / 1e6)--don't multiply by anything as these assets are USD
  					WHEN tp.symbol = 'TUSD' THEN (logs.taker_token_amount_raw / 1e18)--don't multiply by anything as these assets are USD
  					WHEN mp.symbol = 'TUSD' THEN (logs.maker_token_amount_raw / 1e18)--don't multiply by anything as these assets are USD
  					WHEN tp.symbol = 'USDT' THEN (logs.taker_token_amount_raw / 1e6) * tp.price
  					WHEN mp.symbol = 'USDT' THEN (logs.maker_token_amount_raw / 1e6) * mp.price
  					WHEN tp.symbol = 'DAI' THEN (logs.taker_token_amount_raw / 1e18) * tp.price
  					WHEN mp.symbol = 'DAI' THEN (logs.maker_token_amount_raw / 1e18) * mp.price
  					WHEN tp.symbol = 'WETH' THEN (logs.taker_token_amount_raw / 1e18) * tp.price
  					WHEN mp.symbol = 'WETH' THEN (logs.maker_token_amount_raw / 1e18) * mp.price
  					ELSE COALESCE((logs.maker_token_amount_raw / (10^mt.decimals))*mp.price,(logs.taker_token_amount_raw / (10^tt.decimals))*tp.price)
  					END AS volume_usd
  		FROM ERC20BridgeTransfer logs
  		LEFT JOIN prices.usd tp ON date_trunc('minute', TIMESTAMP) = tp.minute
  								AND CASE -- Set Deversifi ETHWrapper to WETH
  										WHEN logs.taker_token IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA -- Set Deversifi USDCWrapper to USDC
  										WHEN logs.taker_token IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
  										ELSE logs.taker_token
  										END = tp.contract_address
  		LEFT JOIN prices.usd mp ON DATE_TRUNC('minute', TIMESTAMP) = mp.minute
  								AND CASE -- Set Deversifi ETHWrapper to WETH
  										WHEN logs.maker_token IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA -- Set Deversifi USDCWrapper to USDC
  										WHEN logs.maker_token IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
  										ELSE logs.maker_token
  										END = mp.contract_address
  		LEFT JOIN erc20.tokens mt ON mt.contract_address = logs.maker_token
  		LEFT JOIN erc20.tokens tt ON tt.contract_address = logs.taker_token
  	),

  	direct_uniswapv2 AS (
  		SELECT 	swap.evt_tx_hash AS tx_hash,
  				swap.evt_index,
  				swap.evt_block_time AS TIMESTAMP,
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
  					WHEN swap."amount0In" > swap."amount1In" THEN swap."amount0In" / (10^t0.decimals)
  					ELSE swap."amount1In"/ (10^t1.decimals)
  					END AS taker_token_amount,
  				CASE
  					WHEN swap."amount0In" > swap."amount1In" THEN swap."amount1Out" / (10^t1.decimals)
  					ELSE swap."amount0Out" / (10^t0.decimals)
  					END AS maker_token_amount,
  			 	'UniswapV2 Direct' AS type
  		FROM uniswap_v2."Pair_evt_Swap" swap
  		LEFT JOIN uniswap_v2."Factory_evt_PairCreated" pair ON pair.pair = swap.contract_address
  		LEFT JOIN erc20.tokens t0 ON t0.contract_address = pair.token0
  		LEFT JOIN erc20.tokens t1 ON t1.contract_address = pair.token1
  		WHERE sender = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
  	),

  	direct_sushiswap AS (
  		SELECT 	swap.evt_tx_hash AS tx_hash,
  				swap.evt_index,
  				swap.evt_block_time AS TIMESTAMP,
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
  					WHEN swap."amount0In" > swap."amount1In" THEN swap."amount0In" / (10^t0.decimals)
  					ELSE swap."amount1In"/ (10^t1.decimals)
  					END AS taker_token_amount,
  				CASE
  					WHEN swap."amount0In" > swap."amount1In" THEN swap."amount1Out" / (10^t1.decimals)
  					ELSE swap."amount0Out" / (10^t0.decimals)
  					END AS maker_token_amount,
  				'Sushiswap Direct' AS type
  		FROM sushi."Pair_evt_Swap" swap
  		LEFT JOIN sushi."Factory_evt_PairCreated" pair ON pair.pair = swap.contract_address
  		LEFT JOIN erc20.tokens t0 ON t0.contract_address = pair.token0
  		LEFT JOIN erc20.tokens t1 ON t1.contract_address = pair.token1
  		WHERE sender = '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
  	),
  	direct_tx AS (
  		SELECT * FROM direct_uniswapv2
  		UNION ALL
  		SELECT * FROM direct_sushiswap
  	),
  	direct_volume AS (
  		SELECT 	dt.*,
  				CASE
  				WHEN tp.symbol = 'USDC' THEN (dt.taker_token_amount)--don't multiply by anything as these assets are USD
  				WHEN mp.symbol = 'USDC' THEN (dt.maker_token_amount)--don't multiply by anything as these assets are USD
  				WHEN tp.symbol = 'TUSD' THEN (dt.taker_token_amount)--don't multiply by anything as these assets are USD
  				WHEN mp.symbol = 'TUSD' THEN (dt.maker_token_amount)--don't multiply by anything as these assets are USD
  				WHEN tp.symbol = 'USDT' THEN (dt.taker_token_amount) * tp.price
  				WHEN mp.symbol = 'USDT' THEN (dt.maker_token_amount) * mp.price
  				WHEN tp.symbol = 'DAI' THEN (dt.taker_token_amount) * tp.price
  				WHEN mp.symbol = 'DAI' THEN (dt.maker_token_amount) * mp.price
  				WHEN tp.symbol = 'WETH' THEN (dt.taker_token_amount) * tp.price
  				WHEN mp.symbol = 'WETH' THEN (dt.maker_token_amount) * mp.price
  				ELSE COALESCE((dt.maker_token_amount)*mp.price,(dt.taker_token_amount)*tp.price)
  				END AS volume_usd
  		FROM direct_tx dt
  		LEFT JOIN prices.usd tp ON date_trunc('minute', TIMESTAMP) = tp.minute
  								AND CASE -- Set Deversifi ETHWrapper to WETH
  									WHEN dt.taker_token IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA -- Set Deversifi USDCWrapper to USDC
  									WHEN dt.taker_token IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
  									ELSE dt.taker_token
  									END = tp.contract_address
  		LEFT JOIN prices.usd mp ON DATE_TRUNC('minute', TIMESTAMP) = mp.minute
  								AND CASE -- Set Deversifi ETHWrapper to WETH
  									WHEN dt.maker_token IN ('\x50cb61afa3f023d17276dcfb35abf85c710d1cff'::BYTEA,'\xaa7427d8f17d87a28f5e1ba3adbb270badbe1011'::BYTEA) THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA -- Set Deversifi USDCWrapper to USDC
  									WHEN dt.maker_token IN ('\x69391cca2e38b845720c7deb694ec837877a8e53'::BYTEA) THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::BYTEA
  									ELSE dt.maker_token
  									END = mp.contract_address
  	)
      select * from direct_volume

  		UNION ALL

  		(
        select COALESCE(bf.tx_hash, vf.transaction_hash) as tx_hash,
             COALESCE(bf.evt_index, vf.evt_index) as evt_index,
             COALESCE(bf."timestamp", vf."timestamp") as "timestamp",
             COALESCE(bf.maker, vf.maker_address) as maker,
             COALESCE(bf.taker, vf.taker_address) as taker,
             COALESCE(bf.taker_token, vf.taker_token) as taker_token,
             COALESCE(bf.maker_token, vf.maker_token) as maker_token,
             COALESCE(bf.taker_token_amount, vf.taker_asset_filled_amount) as taker_token_amount,
             COALESCE(bf.maker_token_amount, vf.maker_asset_filled_amount) as maker_token_amount,
             COALESCE(bf.type, 'Native Fill') as type,
             COALESCE(bf.volume_usd, vf.volume_usd) as volume_usd
         from bridge_fills bf
         full join zeroex."view_fills" vf on vf.transaction_hash=bf.tx_hash
    )

);

CREATE UNIQUE INDEX IF NOT EXISTS zeroex_api_fills_unique ON zeroex.view_0x_api_fills (transaction_hash, evt_index);
CREATE INDEX IF NOT EXISTS zeroex_api_fills_time_index ON zeroex.view_0x_api_fills (timestamp);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY zeroex.view_0x_api_fills')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
