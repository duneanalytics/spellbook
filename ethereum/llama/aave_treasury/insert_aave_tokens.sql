CREATE OR REPLACE FUNCTION llama.insert_aave_tokens(start_time numeric, end_time numeric) RETURNS integer
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

SELECT
token_address, decimals, symbol, erc20_address, erc20_symbol, side
FROM
(
SELECT
a."aToken" AS token_address,
e.decimals AS decimals,
'a' || e.symbol AS symbol,
a.asset AS erc20_address,
e.symbol AS erc20_symbol,
'Deposit' AS side
FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" a --asset is raw, atoken is atoken
	WHERE evt_block_time >= start_time
	AND evt_block_time <= end_time

LEFT JOIN erc20."tokens" e
ON a.asset = e."contract_address"

UNION --ammtokens

SELECT 
    amm."aTokenAddress" AS token_address,
    e.decimals AS decimals,
    'aAmm' || e.symbol AS symbol,
    amm.asset AS erc20_address,
    e.symbol AS erc20_symbol,
    'AMM' AS side
    FROM aave_v2."AaveLendingPoolV2AMM_call_initReserve" amm
    LEFT JOIN erc20."tokens" e
        ON amm.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
	AND evt_block_time >= start_time
	AND evt_block_time <= end_time

UNION --variable debt

SELECT 
    vd."variableDebtToken" AS token_address,
    e.decimals AS decimals,
    'variableDebt' || e.symbol AS symbol,
    vd.asset AS erc20_address,
    e.symbol AS erc20_symbol,
    'Borrow' AS side
    FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" vd
    LEFT JOIN erc20."tokens" e
        ON vd.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
	AND evt_block_time >= start_time
	AND evt_block_time <= end_time

UNION --stable debt 

SELECT 
    sd."stableDebtToken" AS token_address,
    e.decimals AS decimals,
    'stableDebt' || e.symbol AS symbol,
    sd.asset AS erc20_address,
    e.symbol AS erc20_symbol,
    'Borrow' AS side
    FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" sd
    LEFT JOIN erc20."tokens" e
        ON sd.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
	AND evt_block_time >= start_time
	AND evt_block_time <= end_time
) a

    ON CONFLICT (token_address, erc20_address, side) DO UPDATE SET decimals = EXCLUDED.decimals, symbol = EXCLUDED.symbol, erc20_symbol = EXCLUDED.erc20_symbol
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT llama.insert_aave_tokens('2019-01-01'::date,NOW())
WHERE NOT EXISTS (
    SELECT *
    FROM llama.insert_aave_tokens
);

-- tables are so small, so just run everything rather than building in logic for backfilling tokens. Set a 1 year gap to add some constraint.
INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT llama.insert_aave_tokens(
        (SELECT NOW() - interval '1 year'),
        (SELECT NOW());
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
