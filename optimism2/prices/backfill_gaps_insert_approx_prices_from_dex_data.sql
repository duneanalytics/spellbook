--Intended for one-off use to backfill price gaps.

CREATE OR REPLACE FUNCTION prices.backfill_gaps_insert_approx_prices_from_dex_data(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH
hour_gs AS (
SELECT generate_series(DATE_TRUNC('hour',start_time) , DATE_TRUNC('hour',end_time) , '1 hour') AS hour
)
--Fill in gaps

, hour_token_gs AS (
WITH token_list AS (
    SELECT contract_address AS token, symbol, decimals FROM  prices.approx_prices_from_dex_data
    GROUP BY 1,2,3
    )

SELECT 
hour, token, symbol, decimals
FROM
token_list, hour_gs

)



--logic to fill in gaps https://dba.stackexchange.com/questions/186218/carry-over-long-sequence-of-missing-values-with-postgres
, final_prices AS (
SELECT
token AS contract_address, hour
, first_value(median_price) OVER (PARTITION BY token, grp ORDER BY hour) AS median_price
, first_value(num_samples) OVER (PARTITION BY token, grp ORDER BY hour) AS sample_size

, symbol, decimals
     
FROM (
    SELECT 
    gs.hour, gs.token, gs.symbol, gs.decimals, p.median_price, p.sample_size AS num_samples,
        count(p.median_price) OVER (PARTITION BY gs.token ORDER BY gs.hour) AS grp
    FROM hour_token_gs gs
    LEFT JOIN prices.approx_prices_from_dex_data p
        ON gs.hour = p.hour
        AND gs.token = p.contract_address
        AND gs.symbol = p.symbol
        AND gs.decimals = p.decimals
    ) fill
)
,
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
    FROM final_prices
        WHERE median_price IS NOT NULL

    ON CONFLICT (contract_address, hour) DO UPDATE SET median_price = EXCLUDED.median_price, sample_size = EXCLUDED.sample_size
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Monthly backfill starting 11 Nov 2021 (regenesis
--TODO: Add pre-regenesis prices

SELECT prices.backfill_gaps_insert_approx_prices_from_dex_data('2021-11-11'::timestamptz, '2021-12-30'::timestamptz)
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE median_price IS NULL
                    AND hour >= '2021-11-11'::timestamptz AND hour <= '2021-12-30'::timestamptz);

INSERT INTO cron.job (schedule, command)
VALUES ('13,43 * * * *', $$
    SELECT prices.backfill_gaps_insert_approx_prices_from_dex_data(
        (SELECT DATE_TRUNC('hour', now()) - interval '3 days'),
        (SELECT DATE_TRUNC('hour', now()) + interval '1 hour')
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
