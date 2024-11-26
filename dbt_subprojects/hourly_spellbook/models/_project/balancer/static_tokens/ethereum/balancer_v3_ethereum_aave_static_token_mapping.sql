{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'aave_static_tokens_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(
SELECT 
    a.aToken,
    c.aTokenSymbol AS atoken_symbol,
    b.staticAToken AS static_atoken,
    a.staticATokenName AS static_atoken_name,
    a.staticATokenSymbol AS static_atoken_symbol,
    b.underlying AS underlying_token,
    t.symbol AS underlying_token_symbol,
    t.decimals AS underlying_token_decimals
FROM {{ source('aave_ethereum', 'StaticATokenLM_evt_Initialized') }} a
JOIN {{ source('aave_ethereum', 'StaticATokenFactory_evt_StaticTokenCreated') }} b
ON b.staticAToken = a.contract_address
JOIN {{ source('aave_v3_ethereum', 'VariableDebtToken_evt_Initialized') }} c
ON a.aToken = c.contract_address
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND b.underlying = t.contract_address)

SELECT 
    'ethereum' AS blockchain, 
    * 
FROM aave_tokens