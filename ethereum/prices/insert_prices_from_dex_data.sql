CREATE OR REPLACE FUNCTION prices.insert_prices_from_dex_data(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH trades_with_usd_amount AS (
    SELECT
        token_a_address as contract_address,
        symbol,
        decimals,
        usd_amount/(token_a_amount_raw/10^decimals) AS price,
        block_time
    FROM dex.trades
    INNER JOIN erc20.tokens ON contract_address = token_a_address
    WHERE usd_amount  > 0
    AND category = 'DEX'
    AND token_a_amount_raw > 0
    AND block_time >= start_time
    AND block_time < end_time

    UNION ALL

    SELECT
        token_b_address as contract_address,
        symbol,
        decimals,
        usd_amount/(token_b_amount_raw/10^decimals) AS price,
        block_time
    FROM dex.trades
    INNER JOIN erc20.tokens ON contract_address = token_b_address
    WHERE usd_amount  > 0
    AND category = 'DEX'
    AND token_b_amount_raw > 0
    AND block_time >= start_time
    AND block_time < end_time
),
grouped_by_hour AS (
    SELECT
        date_trunc('hour', block_time) as hour,
        contract_address,
        symbol,
        decimals,
        (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS median_price,
        count(1) AS sample_size
    FROM trades_with_usd_amount
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
    FROM prices.prices_from_dex_data
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

rows AS (
    INSERT INTO prices.prices_from_dex_data (
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
    FROM add_data_for_all_hours

    ON CONFLICT (contract_address, hour) DO UPDATE SET median_price = EXCLUDED.median_price, sample_size = EXCLUDED.sample_size
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Monthly backfill starting 1 Jan 2020
SELECT prices.insert_prices_from_dex_data('2020-01-01', '2020-02-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-01-01' and hour < '2020-02-01');

SELECT prices.insert_prices_from_dex_data('2020-02-01', '2020-03-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-02-01' and hour < '2020-03-01');

SELECT prices.insert_prices_from_dex_data('2020-03-01', '2020-04-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-03-01' and hour < '2020-04-01');

SELECT prices.insert_prices_from_dex_data('2020-04-01', '2020-05-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-04-01' and hour < '2020-05-01');

SELECT prices.insert_prices_from_dex_data('2020-05-01', '2020-06-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-05-01' and hour < '2020-06-01');

SELECT prices.insert_prices_from_dex_data('2020-06-01', '2020-07-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-06-01' and hour < '2020-07-01');

SELECT prices.insert_prices_from_dex_data('2020-07-01', '2020-08-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-07-01' and hour < '2020-08-01');

SELECT prices.insert_prices_from_dex_data('2020-08-01', '2020-09-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-08-01' and hour < '2020-09-01');

SELECT prices.insert_prices_from_dex_data('2020-09-01', '2020-10-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-09-01' and hour < '2020-10-01');

SELECT prices.insert_prices_from_dex_data('2020-10-01', '2020-11-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-10-01' and hour < '2020-11-01');

SELECT prices.insert_prices_from_dex_data('2020-11-01', '2020-12-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-11-01' and hour < '2020-12-01');

SELECT prices.insert_prices_from_dex_data('2020-12-01', '2021-01-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2020-12-01' and hour < '2021-01-01');

SELECT prices.insert_prices_from_dex_data('2021-01-01', '2021-02-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-01-01' and hour < '2021-02-01');

SELECT prices.insert_prices_from_dex_data('2021-02-01', '2021-03-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-02-01' and hour < '2021-03-01');

SELECT prices.insert_prices_from_dex_data('2021-03-01', '2021-04-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-03-01' and hour < '2021-04-01');

SELECT prices.insert_prices_from_dex_data('2021-04-01', '2021-05-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-04-01' and hour < '2021-05-01');

SELECT prices.insert_prices_from_dex_data('2021-05-01', '2021-06-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-05-01' and hour < '2021-06-01');

SELECT prices.insert_prices_from_dex_data('2021-06-01', '2021-07-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-06-01' and hour < '2021-07-01');

SELECT prices.insert_prices_from_dex_data('2021-07-01', '2021-08-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-07-01' and hour < '2021-08-01');

SELECT prices.insert_prices_from_dex_data('2021-08-01', '2021-09-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-08-01' and hour < '2021-09-01');

SELECT prices.insert_prices_from_dex_data('2021-09-01', '2021-10-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-09-01' and hour < '2021-10-01');

SELECT prices.insert_prices_from_dex_data('2021-10-01', '2021-11-01')
WHERE NOT EXISTS (SELECT * FROM prices.prices_from_dex_data WHERE hour >= '2021-10-01' and hour < '2021-11-01');

SELECT prices.insert_prices_from_dex_data('2021-11-01', now());

-- Have the insert script run twice every hour at minute 16 and 46
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('16,46 * * * *', $$
    SELECT prices.insert_prices_from_dex_data(
        (SELECT date_trunc('hour', now()) - interval '3 days'),
        (SELECT date_trunc('hour', now())));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
