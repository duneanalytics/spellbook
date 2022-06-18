CREATE OR REPLACE FUNCTION aave.insert_aave_tokens(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_tokens (
      token_address,
      decimals,
      symbol,
      underlying_token_address,
      underlying_token_symbol,
      side,
	token_name,
	program_type
    )

WITH standard_tokens AS (
SELECT
a."aToken" AS token_address,
e.decimals AS decimals,
'a' || e.symbol AS symbol,
a.asset AS underlying_token_address,
e.symbol AS underlying_token_symbol,
'Deposit' AS side
FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" a --asset is raw, atoken is atoken

LEFT JOIN erc20."tokens" e
ON a.asset = e."contract_address"

UNION --ammtokens

SELECT 
    amm."aTokenAddress" AS token_address,
    e.decimals AS decimals,
    'aAmm' || e.symbol AS symbol,
    amm.asset AS underlying_token_address,
    e.symbol AS underlying_token_symbol,
    'AMM' AS side
    FROM aave_v2."AaveLendingPoolV2AMM_call_initReserve" amm
    LEFT JOIN erc20."tokens" e
        ON amm.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
    AND call_block_time >= start_time
    AND call_block_time < end_time

UNION --variable debt

SELECT 
    vd."variableDebtToken" AS token_address,
    e.decimals AS decimals,
    'variableDebt' || e.symbol AS symbol,
    vd.asset AS underlying_token_address,
    e.symbol AS underlying_token_symbol,
    'Borrow' AS side
    FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" vd
    LEFT JOIN erc20."tokens" e
        ON vd.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
    AND evt_block_time >= start_time
    AND evt_block_time < end_time

UNION --stable debt 

SELECT 
    sd."stableDebtToken" AS token_address,
    e.decimals AS decimals,
    'stableDebt' || e.symbol AS symbol,
    sd.asset AS underlying_token_address,
    e.symbol AS underlying_token_symbol,
    'Borrow' AS side
    FROM aave_v2."LendingPoolConfigurator_evt_ReserveInitialized" sd
    LEFT JOIN erc20."tokens" e
        ON sd.asset = e."contract_address"
    WHERE e.decimals IS NOT NULL
    AND evt_block_time >= start_time
    AND evt_block_time < end_time
)

, additional_tokens AS ( --i.e. ARC, RWA, etc.
-- deposit tokens
SELECT 
l.contract_address AS token_address,
bytea2numeric(substring(data from 65 for 32)) AS decimals,
regexp_replace(REPLACE(encode(substring(data from 288 for 128),'escape')::text,'\000',''), '\W', ' ', 'g') AS symbol,
get_address_from_data(topic2) AS underlying_token_address,
e.symbol AS underlying_token_symbol,
'Deposit' AS side,
regexp_replace(REPLACE(encode(substring(data from 224 for 64), 'escape')::text,'\000',''), '\W', ' ', 'g') AS token_name

FROM ethereum.logs l
LEFT JOIN erc20."tokens" e
        ON e.contract_address = get_address_from_data(topic2)
WHERE topic1 = '\xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
AND EXISTS (SELECT 1 FROM erc20."ERC20_evt_Transfer" e WHERE e.contract_address = l.contract_address LIMIT 1)
AND block_time >= start_time
    AND block_time < end_time

UNION ALL
--borrow tokens
SELECT 
l.contract_address AS token_address,
bytea2numeric(substring(data from 33 for 32)) AS decimals,
regexp_replace(replace(encode(substring(data from 256 for 64), 'escape')::text,'\000',''), '\W', ' ', 'g') AS symbol,
get_address_from_data(topic2) AS underlying_token_address,
e.symbol AS underlying_token_symbol,
'Borrow' AS side,
regexp_replace(replace(encode(substring(data from 193 for 64),'escape')::text,'\000',''), '\W', ' ', 'g') AS token_name

FROM ethereum.logs l
LEFT JOIN erc20."tokens" e
        ON e.contract_address = get_address_from_data(topic2)
WHERE topic1 = '\x40251fbfb6656cfa65a00d7879029fec1fad21d28fdcff2f4f68f52795b74f2c'
AND block_time >= start_time
    AND block_time < end_time


)
SELECT *
FROM (
    SELECT
    token_address, decimals::INT, symbol, underlying_token_address, underlying_token_symbol, side, token_name,
    TRIM(
        CASE
            WHEN token_name LIKE '%interest%' THEN SPLIT_PART(token_name,'interest',1)
            WHEN token_name LIKE '%Interest%' THEN SPLIT_PART(token_name,'Interest',1)
            WHEN token_name LIKE '%market%' THEN SPLIT_PART(token_name,'market',1)
            WHEN token_name LIKE '%Market%' THEN SPLIT_PART(token_name,'Market',1)
            WHEN token_name LIKE '%variable%' THEN SPLIT_PART(token_name,'variable',1)
            WHEN token_name LIKE '%stable%' THEN SPLIT_PART(token_name,'stable',1)
        END
        ) AS program_type
    FROM additional_tokens
    
    UNION ALL
    
    SELECT
    token_address, decimals::INT, symbol, underlying_token_address, underlying_token_symbol, side, NULL AS token_name,
    CASE WHEN symbol LIKE 'aAMM%' THEN 'Aave AMM' ELSE 'Aave' END AS program_type
    FROM standard_tokens s
    WHERE token_address NOT IN (SELECT token_address FROM additional_tokens)
    ) aat
GROUP BY 1,2,3,4,5,6,7,8

    ON CONFLICT (token_address, underlying_token_address, side) DO UPDATE SET
	decimals = EXCLUDED.decimals, symbol = EXCLUDED.symbol, underlying_token_symbol = EXCLUDED.underlying_token_symbol,
	token_name = EXCLUDED.token_name, program_type = EXCLUDED.program_type
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT aave.insert_aave_tokens('2019-01-01'::timestamptz,NOW())
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_tokens
);

-- tables are so small, so just run everything rather than building in logic for backfilling tokens. Set a 1 year gap to add some constraint.
INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT aave.insert_aave_tokens(
        (SELECT NOW() - interval '1 year'),
        (SELECT NOW());
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
