{{
    config(
        schema = 'balancer_v3_gnosis',
        alias = 'aave_static_tokens_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(
SELECT DISTINCT
    a.aToken, 
    SUBSTRING(a.staticATokenSymbol, 5, 99) AS atoken_symbol,
    a.contract_address AS static_atoken,
    a.staticATokenName AS static_atoken_name,,
    a.staticATokenSymbol AS static_atoken_symbol,
    t.contract_address  AS underlying_token,
    t.symbol AS underlying_token_symbol,
    t.decimals AS underlying_token_decimals
FROM {{ source('aave_v3_gnosis', 'AToken_evt_Initialized') }} a
JOIN {{ source('prices', 'usd') }} t ON LOWER(SUBSTRING(a.staticATokenSymbol, 9, 99)) = LOWER(t.symbol)
WHERE aToken IS NOT NULL
AND blockchain = 'gnosis')

SELECT 
    'gnosis' AS blockchain, 
    * 
FROM aave_tokens