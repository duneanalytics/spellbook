CREATE OR REPLACE FUNCTION prices.insert_approx_prices_from_dex_data(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH 
chainlink_prices AS (
	SELECT DATE_TRUNC('hour',hour) AS hour, underlying_token_address AS contract_address, symbol, decimals, underlying_token_price AS price
	FROM chainlink.view_price_feeds cp
	INNER JOIN erc20.tokens e
		ON e.contract_address = cp.underlying_token_address
	WHERE hour >= start_time
	AND hour <= end_time
	
	)

-- if this is the run before dex trades, then this CTE would be null and we only pull chainlink prices (this is by design)
, trades_with_usd_amount AS (
    SELECT
	tx_hash,
        token_a_address as contract_address,
        symbol,
        decimals,
        usd_amount/(token_a_amount_raw/10^decimals) AS price,
        block_time
    FROM dex.trades
    INNER JOIN erc20.tokens ON contract_address = token_a_address
    WHERE usd_amount > 0
    AND token_a_amount_raw > 100 -- filter out small spam
    AND block_time >= start_time
    AND block_time < end_time
AND 1 =
	(CASE
	 -- For the following DEXs, grab tokens for all trades
	 	WHEN project IN ('Uniswap','1inch','0x API','Matcha','Zipswap','Velodrome','Curve','Sushiswap','Slingshot') THEN 1
	 -- For Beethoven X, only grab BEETS and BAL for now.
	 	WHEN project = 'Beethoven X'
	 		AND
	 		(
				token_a_address IN ('\xFE8B128bA8C78aabC59d4c64cEE7fF28e9379921','\x97513e975a7fA9072c72C92d8000B0dB90b163c5')
				OR
				token_b_address IN ('\xFE8B128bA8C78aabC59d4c64cEE7fF28e9379921','\x97513e975a7fA9072c72C92d8000B0dB90b163c5')
			) THEN 1
	 ELSE 0
	 END
	 )
	 
	GROUP BY 1,2,3,4,5,6 --remove dupes

    UNION ALL

    SELECT
	tx_hash,
        token_b_address as contract_address,
        symbol,
        decimals,
        usd_amount/(token_b_amount_raw/10^decimals) AS price,
        block_time
    FROM dex.trades
    INNER JOIN erc20.tokens ON contract_address = token_b_address
    WHERE usd_amount  > 0
    AND token_b_amount_raw > 100 -- filter out small spam
    AND block_time >= start_time
    AND block_time < end_time
AND 1 =
	(CASE
	 -- For the following DEXs, grab tokens for all trades
	 	WHEN project IN ('Uniswap','1inch','0x API','Matcha','Zipswap','Velodrome','Curve','Sushiswap','Slingshot') THEN 1
	 -- For Beethoven X, only grab BEETS and BAL for now.
	 	WHEN project = 'Beethoven X'
	 		AND
	 		(
				token_a_address IN ('\xFE8B128bA8C78aabC59d4c64cEE7fF28e9379921','\x97513e975a7fA9072c72C92d8000B0dB90b163c5')
				OR
				token_b_address IN ('\xFE8B128bA8C78aabC59d4c64cEE7fF28e9379921','\x97513e975a7fA9072c72C92d8000B0dB90b163c5')
			) THEN 1
	 ELSE 0
	 END
	 )
	
	GROUP BY 1,2,3,4,5,6 --remove dupes
),
grouped_by_hour AS (

	SELECT
	hour, contract_address, symbol, decimals, price AS median_price, 1 AS sample_size
	FROM chainlink_prices -- use these as the main source of truth
	
	UNION ALL
	
    SELECT
        date_trunc('hour', block_time) as hour,
        contract_address,
        symbol,
        decimals,
        (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS median_price,
        count(1) AS sample_size
    FROM trades_with_usd_amount t
	WHERE NOT EXISTS (SELECT 1 FROM chainlink_prices c WHERE date_trunc('hour', block_time) =c.hour AND t.contract_address=c.contract_address)
    GROUP BY 1, 2, 3, 4
    -- Get previous entries from the table
    -- This is necessary to provide price continuity for tokens with low swap volume
    -- when performing historic backfill.
    UNION ALL
    SELECT
        hour,
        contract_address,
        symbol,
        decimals,
        median_price,
        sample_size
    FROM prices.approx_prices_from_dex_data
    WHERE hour = (select start_time - INTERVAL '1 hour')
),
-- The SQL code in `leaddata`, `generate_hours` and `add_data_for_all_hours`
-- sets the median_price to the price of the previous hour in case that there
-- are no swaps in an hour for a token.
leaddata as 
(
    SELECT
        hour,
        contract_address,
        symbol,
        decimals,
        median_price,
        sample_size,
        lead(hour, 1, end_time) OVER (PARTITION BY contract_address ORDER BY hour asc) AS next_hour
    FROM grouped_by_hour
--    WHERE sample_size > 0
),
generate_hours AS
(
    SELECT hour from generate_series(start_time, end_time, '1 hour') g(hour)
),
add_data_for_all_hours as 
(
    SELECT
    gen.hour as hour,
    contract_address,
    symbol,
    decimals,
    median_price,
    CASE WHEN gen.hour = data.hour THEN sample_size ELSE 0 END AS sample_size
    FROM leaddata data
    INNER JOIN generate_hours gen ON data.hour <= gen.hour
    AND gen.hour < data.next_hour -- Yields an observation for every hour after the first transfer until the next hour with transfer
),

dex_price_bridge_tokens AS (
SELECT DATE_TRUNC('hour', pr.hour) AS hour, "bridge_token" AS token, "bridge_symbol" AS symbol, "bridge_decimals" AS decimals, median_price * price_ratio AS median_price, pr.sample_size,
DENSE_RANK() OVER (PARTITION BY bridge_token ORDER BY pr.hour DESC) AS hrank

FROM prices.hourly_bridge_token_price_ratios pr

INNER JOIN add_data_for_all_hours p
        ON pr.erc20_token = p.contract_address
        AND DATE_TRUNC('hour',pr.hour) = p.hour

),

rows AS (
    INSERT INTO prices.approx_prices_from_dex_data (
        contract_address,
        hour,
        median_price,
        sample_size,
        symbol,
        decimals
    )

    SELECT 
        contract_address,
        hour,
        median_price,
        sample_size,
        symbol,
        decimals
    FROM add_data_for_all_hours a
-- Don't pull trades for bridge tokens
WHERE NOT EXISTS (
	SELECT 1 FROM dex_price_bridge_tokens pr
	  WHERE pr.token = a.contract_address
		AND DATE_TRUNC('hour',pr.hour) = a.hour
	  )

UNION ALL

	SELECT token AS contract_address, hour, median_price, sample_size, symbol, decimals
	FROM dex_price_bridge_tokens

    ON CONFLICT (contract_address, hour) DO UPDATE SET median_price = EXCLUDED.median_price, sample_size = EXCLUDED.sample_size
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;



-- Monthly backfill starting 11 Nov 2021 (regenesis
SELECT prices.insert_approx_prices_from_dex_data('2021-11-01', '2021-12-01')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-11-01' and hour < '2021-12-01');

SELECT prices.insert_approx_prices_from_dex_data('2021-12-01', '2021-12-31')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-12-01' and hour < '2021-12-31');

--Splitting Jan/Feb in to pieces since there was higher tx volume
SELECT prices.insert_approx_prices_from_dex_data('2021-12-31', '2022-01-10')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-12-31' and hour < '2022-01-10');
SELECT prices.insert_approx_prices_from_dex_data('2022-01-10', '2022-01-20')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2022-01-10' and hour < '2022-01-20');
SELECT prices.insert_approx_prices_from_dex_data('2022-01-20', '2022-01-31')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2022-01-20' and hour < '2022-01-31');
SELECT prices.insert_approx_prices_from_dex_data('2022-01-31', '2022-02-14')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2022-01-31' and hour < '2022-02-14');

SELECT prices.insert_approx_prices_from_dex_data('2022-02-14', NOW());
--WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2022-02-14');

-- CRON inserts happen in the main dex update job
/*
-- Have the insert script run twice every hour at minute 16 and 46
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('16,46 * * * *', $$
    SELECT prices.insert_approx_prices_from_dex_data(
        (SELECT MAX(hour) - interval '1 hour' FROM prices.approx_prices_from_dex_data),
        (SELECT DATE_TRUNC('hour', now()) + interval '1 hour')
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

--once per day run of the last 30 days to handle for new tokens
INSERT INTO cron.job (schedule, command)
VALUES ('1 0 * * *', $$
    SELECT prices.insert_approx_prices_from_dex_data(
        (SELECT MAX(hour) - interval '30 days' FROM prices.approx_prices_from_dex_data),
        (SELECT DATE_TRUNC('hour', now()) + interval '1 hour')
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
