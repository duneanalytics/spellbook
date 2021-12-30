CREATE OR REPLACE FUNCTION llama.insert_aave_tokens(start_block numeric, end_block numeric) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO llama.aave_tokens (
      token_address,
      decimals,
      symbol,
      erc20_address,
      erc20_symbol,
      side
    )


	--https://dba.stackexchange.com/questions/186218/carry-over-long-sequence-of-missing-values-with-postgres

	SELECT 
  --stuff to insert

	FROM events

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT llama.insert_aave_tokens(1,1000)
WHERE NOT EXISTS (
    SELECT *
    FROM llama.insert_aave_tokens
    WHERE block_number = 1000
);

INSERT INTO cron.job (schedule, command)
VALUES ('5,25,45 * * * *', $$
    SELECT llama.insert_aave_tokens(
        (SELECT NOW() - interval '3 days'),
        (SELECT NOW());
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
