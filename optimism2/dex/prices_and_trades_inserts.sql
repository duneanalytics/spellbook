INSERT INTO cron.job (schedule, command)
VALUES ('15,30,45,59 * * * *', $$
--Chainlink Updates
	SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '1 hour' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        (SELECT max_time FROM ovm2.view_last_updated) -- when the db was last updated, this handles for the db getting stuck
        );
--First Prices Run. We expect this to only pull in Chainlink updates
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '1 hour' FROM prices.approx_prices_from_dex_data),
        	(SELECT max_time FROM ovm2.view_last_updated) -- when the db was last updated, this handles for the db getting stuck
    	);
	
----------
--DEX Inserts. These should only pull in prices where there is a Chainlink oracle.
	SELECT dex.insert_uniswap_v3( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Uniswap' AND version = '3'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_oneinch( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='1inch'), (SELECT max_time FROM ovm2.view_last_updated), 0);
	SELECT dex.insert_zeroex( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project IN ('0x API', 'Matcha')), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_zipswap( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Zipswap'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_curve( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Curve'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_clipper( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Clipper' AND version = '1'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_clipper_v2( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Clipper' AND version = '2'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_kwenta( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Kwenta'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_wardenswap( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='WardenSwap' AND version = '2'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_rubicon( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Rubicon'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_velodrome( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Velodrome'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_beethoven_x( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Beethoven X'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_sushiswap( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Sushiswap'), (SELECT max_time FROM ovm2.view_last_updated) );
	SELECT dex.insert_slingshot( (SELECT COALESCE(max(block_time),'2021-11-11'::date) - interval '1 hour' FROM dex.trades WHERE project='Slingshot'), (SELECT max_time FROM ovm2.view_last_updated) );
	--ADD REMAINING DEX INSERTS HERE
--END DEX Inserts
----------
	
-- Second Prices Run. We expect this to pull in prices for all tokens that interacted with a token included in Chainlink oralces.
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '2 hours' FROM prices.approx_prices_from_dex_data),
        	(SELECT max_time FROM ovm2.view_last_updated)
    	);
-- Backfill DEX Trades Run. This updates usd_amount based on our latest prices run.
	SELECT dex.backfill_insert_missing_prices(
		(SELECT max(block_time) - interval '2 hours' FROM dex.trades), --adding extra 1hr buffer for safety
		(SELECT max_time FROM ovm2.view_last_updated)
		);
-- Third Prices Run. We expect this to pull in the remining prices (Oracles + Interacted with Tokens + Next level of tokens).
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '3 hours' FROM prices.approx_prices_from_dex_data),
        	(SELECT max_time FROM ovm2.view_last_updated)
    	);
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

-- Longer periodic backfill once per day (i.e. when we add new tokens or feeds)
INSERT INTO cron.job (schedule, command)
VALUES ('7 1 * * *', $$
--Chainlink Updates
	SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '30 days' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        (SELECT max_time FROM ovm2.view_last_updated)
        );
-- Third Prices Run. We expect this to pull in the remining prices (Oracles + Interacted with Tokens + Next level of tokens).
	SELECT prices.insert_approx_prices_from_dex_data(
        	(SELECT MAX(hour) - interval '30 days' FROM prices.approx_prices_from_dex_data),
        	(SELECT max_time FROM ovm2.view_last_updated)
    	);
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
