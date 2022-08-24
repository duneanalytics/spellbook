CREATE TABLE IF NOT EXISTS aave.tokens (   
    version text,	
    symbol text,
    token bytea PRIMARY KEY,
    a_token bytea,
    stable_debt_token bytea,
    variable_debt_token bytea,
    decimals numeric,
    UNIQUE (token)
    
);

CREATE OR REPLACE FUNCTION aave.insert_tokens(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.tokens (
    version,	    
    symbol,
    token,
    a_token,
    stable_debt_token,
    variable_debt_token,
    decimals
    )
    ((SELECT
    '2' AS version,
    symbol,
    asset AS token,
    "aToken" AS a_token,
    "stableDebtToken" AS stable_debt_token,
    "variableDebtToken" AS variable_debt_token,
    decimals
FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" pools
LEFT JOIN (
    SELECT 
        contract_address, 
        tokens.decimals,
        CASE WHEN (symbol = 'ETH'::text) THEN 'WETH'::text ELSE symbol
        END AS "symbol"
    FROM erc20.tokens
) erc20tokens
ON pools.asset = erc20tokens.contract_address
ORDER BY symbol

    
    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_tokens(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.tokens
);


INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT aave.tokens(
        (SELECT NOW() - interval '1 year'),
        (SELECT NOW());
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
