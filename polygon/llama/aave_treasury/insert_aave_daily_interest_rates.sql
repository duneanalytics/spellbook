CREATE OR REPLACE FUNCTION llama.insert_aave_daily_interest_rates(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
	start_time_day timestamptz := DATE_TRUNC('day',start_time);
	end_time_day timestamptz := DATE_TRUNC('day',end_time) + interval '1 day'; --since we trunc to day
BEGIN
WITH rows AS (
    INSERT INTO llama.aave_daily_interest_rates (
	day,
	token_address,
	daily_change,
	starting_balance,
	interest_rate_apr,
	int_earned,
	total_bal
    )
	


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
SELECT llama.insert_aave_daily_interest_rates(DATE_TRUNC('day','2021-04-13'::timestamptz),DATE_TRUNC('day','2021-12-31'::timestamptz) )
WHERE NOT EXISTS (
    SELECT *
    FROM llama.aave_daily_interest_rates
);

INSERT INTO cron.job (schedule, command)
VALUES ('16,46 * * * *', $$
    SELECT llama.insert_aave_daily_interest_rates(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
