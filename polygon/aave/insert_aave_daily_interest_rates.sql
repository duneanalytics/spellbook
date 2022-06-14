CREATE OR REPLACE FUNCTION aave.insert_aave_daily_interest_rates(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
	start_time_day timestamptz := DATE_TRUNC('day',start_time);
	end_time_day timestamptz := DATE_TRUNC('day',end_time) + interval '1 day'; --since we trunc to day
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_daily_interest_rates (
	underlying_token,
	    token,
	    day,
	    interest_rate_raw,
	    interest_rate_ray,
	    interest_rate_apr
    )
SELECT
underlying_token, token, day, interest_rate_raw, interest_rate_ray, interest_rate_apr

FROM
( 
SELECT 
underlying_token, token, gs.day AS day, interest_rate_raw, interest_rate_ray --ray matches aave UI
,((1+interest_rate_ray)^(1.0/365.0)-1) AS interest_rate_apr --convert apy to daily apr
FROM (
SELECT
"reserve" AS underlying_token, a."output_aTokenAddress" AS token, day,
lead(day, 1, DATE_TRUNC('day',now() + '1 day'::interval) ) OVER (PARTITION BY "reserve"
                            ORDER BY day asc) AS next_day,
interest_rate AS interest_rate_raw,
interest_rate/(10^27) AS interest_rate_ray
FROM
(   SELECT
    "reserve",
    DATE_TRUNC('day',"evt_block_time") AS day,
    AVG("liquidityRate") AS interest_rate
    FROM
    aave_v2."LendingPool_evt_ReserveDataUpdated"
    GROUP BY 1,2
) ra
LEFT JOIN ( SELECT DISTINCT asset, "output_aTokenAddress"
            FROM aave_v2."ProtocolDataProvider_call_getReserveTokensAddresses"
            WHERE "output_aTokenAddress" IS NOT NULL
            ) a --asset is raw, atoken is atoken
ON ra."reserve" = a.asset

) r
INNER JOIN 
(SELECT generate_series(start_time_day, end_time_day, '1 day') AS day) gs
ON r.day <= gs.day
AND gs.day < r.next_day
) f	


    ON CONFLICT (underlying_token, token, day) DO UPDATE SET
    
	interest_rate_raw = EXCLUDED.interest_rate_raw,
	interest_rate_ray = EXCLUDED.interest_rate_ray,
	interest_rate_apr = EXCLUDED.interest_rate_apr
	
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT aave.insert_aave_daily_interest_rates(
    '2021-04-13'
    ,'2022-01-01'
    )
;

-- Get the table started
SELECT aave.insert_aave_daily_interest_rates(
    '2022-01-01'
    ,NOW() 
    )
;

INSERT INTO cron.job (schedule, command)
VALUES ('14,44 * * * *', $$
    SELECT aave.insert_aave_daily_interest_rates(
        (SELECT NOW() - interval '3 days'),
        (SELECT NOW());
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;