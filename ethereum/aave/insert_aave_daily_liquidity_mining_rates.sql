CREATE OR REPLACE FUNCTION aave.insert_aave_daily_liquidity_mining_rates(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;

BEGIN
WITH rows AS (
    INSERT INTO aave.aave_daily_liquidity_mining_rates (
    day,
    token_address,
    lm_reward_apr_yr,
    lm_reward_apr_daily,
    lm_token_yr_raw,
    lm_token_daily_raw,
    aave_decimals
    )

WITH lm_updates AS (
SELECT day,"asset", "emission", "evt_block_time",
    lead(day, 1, DATE_TRUNC('day',now() + '1 day'::interval) ) OVER (PARTITION BY "asset"
                            ORDER BY day asc) AS next_day
    FROM (
        SELECT 
        DATE_TRUNC('day',"evt_block_time") AS day, "asset", "emission", "evt_block_time"
            FROM aave_v2."IncentivesController_evt_AssetConfigUpdated" 
        ) lm
)

, prices AS (
SELECT DATE_TRUNC('day',"minute") AS p_day, "contract_address", decimals, symbol, AVG(price) AS price --avg should be the best time-weighted way to approximate rates
    FROM prices.usd
    WHERE contract_address IN (SELECT at."underlying_token_address" FROM lm_updates l INNER JOIN aave."aave_tokens" at
                                                                        ON l.asset = at."token_address"
                                UNION ALL SELECT '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'::bytea --AAVE
                                )
    AND "minute" >= start_time AND "minute" <= end_time
    GROUP BY 1,2,3,4
    
UNION ALL

SELECT DATE_TRUNC('day',"minute") AS p_day, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea AS "contract_address", 18 AS decimals, symbol, AVG(price) AS price --avg should be the best time-weighted way to approximate rates
    FROM  prices."layer1_usd_eth"
    WHERE "minute" >= start_time AND "minute" <= end_time
    GROUP BY 1,2,3,4
)

SELECT
d.day, d.token_address,
d.lm_reward_apr AS lm_reward_apr_yr,
d.lm_reward_apr/365.00 AS lm_reward_apr_daily,
d.lm_token_yr_raw, --needs to eventually be divided by aave's decimals
d.lm_token_yr_raw/365.00 AS lm_token_daily_raw,
aave_decimals::smallint

FROM
(
SELECT
atb.day, atb.token_address,
((i.emission * (60*60*24*365)) * paave.price/eth.price * 10^tok.decimals)
/(atb.total_balance * tok.price/eth.price  * 10^paave.decimals) AS lm_reward_apr,

(i.emission * (60*60*24*365)) /*/ 10^aave.decimals)*/
/ (atb.total_balance / 10^tok.decimals) AS lm_token_yr_raw,
paave.decimals AS aave_decimals

FROM
aave.aave_daily_atoken_balances atb

INNER JOIN aave."aave_tokens" at
ON atb.token_address = at."token_address"

INNER JOIN lm_updates i
ON i.asset = atb.token_address --get the lm rate on the day
AND atb.day >= i.day
AND atb.day < i.next_day

INNER JOIN prices paave
ON paave.p_day = atb.day
AND paave."contract_address" = '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'

INNER JOIN prices tok
ON tok.p_day = atb.day
AND tok."contract_address" = at."underlying_token_address"

INNER JOIN prices eth
ON eth.p_day = atb.day
AND eth.contract_address = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
	
WHERE atb.day >= start_time AND atb.day <= end_time

) d


    ON CONFLICT (day, token_address) DO UPDATE SET
    
    lm_reward_apr_yr = EXCLUDED.lm_reward_apr_yr,
    lm_reward_apr_daily = EXCLUDED.lm_reward_apr_daily,
    lm_token_yr_raw = EXCLUDED.lm_token_yr_raw,
    lm_token_daily_raw = EXCLUDED.lm_token_daily_raw,
    aave_decimals = EXCLUDED.aave_decimals
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT aave.insert_aave_daily_liquidity_mining_rates(DATE_TRUNC('day','2020-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_daily_liquidity_mining_rates
);

INSERT INTO cron.job (schedule, command)
VALUES ('16,46 * * * *', $$
    SELECT aave.insert_aave_daily_liquidity_mining_rates(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
