CREATE OR REPLACE FUNCTION llama.insert_aave_fees_by_day(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO llama.aave_fees_by_day (

    )

    ON CONFLICT (day, contract_address) DO UPDATE SET
	borrow_fees_originated = EXCLUDED.borrow_fees_originated,
	repay_fees = EXCLUDED.repay_fees,
	liquidation_fees = EXCLUDED.liquidation_fees,
	flashloan_v1_fees = EXCLUDED.flashloan_v1_fees,
	flashloan_v2_fees = EXCLUDED.flashloan_v2_fees,
	swap_fees = EXCLUDED.swap_fees,
	lend_burn_fees = EXCLUDED.lend_burn_fees,
	deployer_in = EXCLUDED.deployer_in,
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT llama.insert_aave_fees_by_day(DATE_TRUNC('day','2019-01-01'::timestamptz),DATE_TRUNC('day','2021-12-31') )
WHERE NOT EXISTS (
    SELECT *
    FROM llama.aave_fees_by_day
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT llama.insert_aave_fees_by_day(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
