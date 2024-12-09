{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'erc4626_tokens_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(
SELECT 
    b.staticAToken AS erc4626_token,
    a.staticATokenName AS erc4626_token_name,
    a.staticATokenSymbol AS erc4626_token_symbol,
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
AND b.underlying = t.contract_address),

morpho_tokens AS(
SELECT DISTINCT
    a.metaMorpho AS erc4626_token,
    a.name AS erc4626_token_name,
    a.symbol AS erc4626_token_symbol,
    a.asset AS underlying_token,
    t.symbol AS underlying_token_symbol,
    t.decimals AS underlying_token_decimals
FROM {{ source('metamorpho_factory_ethereum', 'MetaMorphoFactory_evt_CreateMetaMorpho') }} a
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND a.asset = t.contract_address)

SELECT 
    'ethereum' AS blockchain, 
    * 
FROM aave_tokens

UNION 

SELECT 
    'ethereum' AS blockchain, 
    * 
FROM morpho_tokens