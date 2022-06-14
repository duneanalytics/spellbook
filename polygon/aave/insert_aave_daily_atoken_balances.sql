CREATE OR REPLACE FUNCTION aave.insert_aave_daily_atoken_balances(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
	start_time_day timestamptz := DATE_TRUNC('day',start_time);
	end_time_day timestamptz := DATE_TRUNC('day',end_time) + interval '1 day'; --since we trunc to day
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_daily_atoken_balances (
	day,
	token_address,
	daily_change,
	starting_balance,
	interest_rate_apr,
	int_earned,
	total_bal
    )
	
WITH RECURSIVE 
gs AS --doing a series so that we don't skip days when there's no deposits/withraws
(
SELECT DISTINCT d.token_address AS token_address, gs.day FROM aave."aave_tokens" d
INNER JOIN 
(SELECT generate_series(start_time_day, end_time_day, '1 day') AS day) gs -- Generate all days since the first contract
ON 1=1
)

, rawbals AS ( --raw balances without interest. Doing these in a cte helps with runtime
SELECT
gs.day,gs."token_address" AS "contract_address", COALESCE(a.value,0) AS value,
COALESCE("interest_rate_apr",0) AS interest_rate_apr, drank
FROM (
SELECT pre.*,
DENSE_RANK() OVER (PARTITION BY pre."contract_address" ORDER BY day ASC) AS drank
FROM
(
    SELECT
    DATE_TRUNC('day',e."evt_block_time") AS day, e."contract_address",
    SUM(
    CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN "value"
        WHEN "to" = '\x0000000000000000000000000000000000000000' THEN -"value"
        ELSE 0 END) AS value
    FROM erc20."ERC20_evt_Transfer" e
    INNER JOIN aave."aave_tokens" at
        ON e."contract_address" = at."token_address"
    
    WHERE ("from" = '\x0000000000000000000000000000000000000000'
    OR "to" = '\x0000000000000000000000000000000000000000')
	AND e.evt_block_time >= start_time_day AND e.evt_block_time <= end_time_day
    GROUP BY 1,2

UNION ALL --start balances
	SELECT day, token_address, total_bal
	FROM (
		SELECT
		day, token_address, total_bal,
			DENSE_RANK() OVER (PARTITION BY token_address ORDER BY day DESC) AS rnk
		FROM aave.aave_daily_atoken_balances
		WHERE day < start_time_day
	) start_bal
	WHERE rnk = 1
) pre
) a
RIGHT JOIN gs
ON gs.day = a.day
AND a."contract_address" = gs."token_address"
INNER JOIN aave."aave_daily_interest_rates" di
ON di.day = gs.day
AND di."token" = gs."token_address"
)

,abalances AS --recursive query so that we can compound atoken interest each day.
(
SELECT day, contract_address,value, interest_rate_apr,
value::decimal AS value_diff,
value::decimal AS starting_value, value::decimal AS pre_int_balance,
value*interest_rate_apr::decimal AS int_earned,
value::decimal + (value*interest_rate_apr::decimal) AS total_bal
FROM rawbals
WHERE drank = 1 --first daily entry for each token

UNION

SELECT
gr.day,gr."contract_address", gr.value,gr."interest_rate_apr",
gr.value,
COALESCE(c.total_bal,0)::decimal AS starting_value,
(COALESCE(c.total_bal,0) + gr.value)::decimal AS pre_int_balance,
( (COALESCE(c.total_bal,0) + gr.value)::decimal * COALESCE(gr."interest_rate_apr",0)::decimal ) AS int_earned,
( (COALESCE(c.total_bal,0) + gr.value)::decimal )
+
( (COALESCE(c.total_bal,0) + gr.value)::decimal * COALESCE(gr."interest_rate_apr",0)::decimal )
AS total_bal
FROM rawbals gr

INNER JOIN abalances c --yesterday
ON gr.contract_address = c.contract_address
AND gr.day = c.day + '1 day'::interval
)



SELECT day, contract_address AS token_address, value_diff AS daily_change,
starting_value AS starting_balance, interest_rate_apr, int_earned, total_bal
FROM abalances


    ON CONFLICT (day, token_address) DO UPDATE SET
    
	daily_change = EXCLUDED.daily_change,
	starting_balance = EXCLUDED.starting_balance,
	interest_rate_apr = EXCLUDED.interest_rate_apr,
	int_earned = EXCLUDED.int_earned,
	total_bal = EXCLUDED.total_bal
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT aave.insert_aave_daily_atoken_balances(
	'2021-04-13'
	,'2022-01-01'
	)
;

-- Get the table started
SELECT aave.insert_aave_daily_atoken_balances(
	'2022-01-01'
	,NOW()
	)
;

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT aave.insert_aave_daily_atoken_balances(
        (SELECT NOW() - interval '3 days'),
        (SELECT NOW());	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;