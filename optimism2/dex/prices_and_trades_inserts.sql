INSERT INTO cron.job (schedule, command)
VALUES ('15,30,45,59 * * * *', $$
--Chainlink Updates
	SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '1 hour' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        now()
        );
--First Prices Run. We expect this to only pull in Chainlink updates
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '1 hour' FROM prices.approx_prices_from_dex_data),
        	now()
    	);
	
----------
--DEX Inserts. These should only pull in prices where there is a Chainlink oracle.
	SELECT dex.insert_uniswap_v3( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Uniswap' AND version = '3'), now() );
	SELECT dex.insert_oneinch( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='1inch'), now(), 0);
	SELECT dex.insert_zeroex( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project IN ('0x API', 'Matcha')), now() );
	SELECT dex.insert_zipswap( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Zipswap'), now() );
	SELECT dex.insert_curve( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Curve'), now() );
	SELECT dex.insert_clipper( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Clipper'), now() );
-- 	SELECT dex.insert_kwenta( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Kwenta'), now() );
	SELECT dex.insert_wardenswap( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='WardenSwap' AND version = '2'), now() );
	SELECT dex.insert_rubicon( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Rubicon'), now() );
	SELECT dex.insert_velodrome( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Velodrome'), now() );
	SELECT dex.insert_beethoven_x( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Beethoven X'), now() );
	SELECT dex.insert_sushiswap( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Sushiswap'), now() );
	SELECT dex.insert_slingshot( (SELECT max(block_time) - interval '1 hour' FROM dex.trades WHERE project='Slingshot'), now() );
    );
	--ADD REMAINING DEX INSERTS HERE
--END DEX Inserts
----------
	
-- Second Prices Run. We expect this to pull in prices for all tokens that interacted with a token included in Chainlink oralces.
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '2 hours' FROM prices.approx_prices_from_dex_data),
        	(SELECT now() )
    	);
-- Backfill DEX Trades Run. This updates usd_amount based on our latest prices run.
	SELECT dex.backfill_insert_missing_prices(
		(SELECT max(block_time) - interval '2 hours' FROM dex.trades), --adding extra 1hr buffer for safety
		now()
		);
-- Third Prices Run. We expect this to pull in the remining prices (Oracles + Interacted with Tokens + Next level of tokens).
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '3 hours' FROM prices.approx_prices_from_dex_data),
        	now()
    	);
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

-- Longer periodic backfill once per day (i.e. when we add new tokens or feeds)
INSERT INTO cron.job (schedule, command)
VALUES ('7 1 * * *', $$
--Chainlink Updates
	SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '30 days' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        now()
        );
-- Third Prices Run. We expect this to pull in the remining prices (Oracles + Interacted with Tokens + Next level of tokens).
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '30 days' FROM prices.approx_prices_from_dex_data),
        	(SELECT now() )
    	);
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
