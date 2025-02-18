{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(
SELECT 
    b.staticaToken AS erc4626_token,
    a.staticaTokenName AS erc4626_token_name,
    a.staticaTokenSymbol AS erc4626_token_symbol,
    b.underlying AS underlying_token,
    t.symbol AS underlying_token_symbol,
    t.decimals AS decimals
FROM {{ source('aave_ethereum', 'StaticATokenLM_evt_Initialized') }} a
JOIN {{ source('aave_ethereum', 'StaticATokenFactory_evt_StaticTokenCreated') }} b
ON b.staticaToken = a.contract_address
JOIN {{ source('aave_v3_ethereum', 'VariableDebtToken_evt_Initialized') }} c
ON a.aToken = c.contract_address
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND b.underlying = t.contract_address

UNION 

SELECT 
    b.stataToken AS erc4626_token,
    t2.symbol AS erc4626_token_name,
    t2.symbol AS erc4626_token_symbol,
    b.underlying AS underlying_token,
    t1.symbol AS underlying_token_symbol,
    t2.decimals AS decimals
FROM {{ source('aave_ethereum', 'StataTokenV2_evt_Initialized') }} a
JOIN {{ source('aave_ethereum', 'StataTokenFactory_evt_StataTokenCreated') }} b
ON b.stataToken = a.contract_address
JOIN {{ source('tokens', 'erc20') }} t1
ON t1.blockchain = 'ethereum'
AND b.underlying = t1.contract_address
JOIN {{ source('tokens', 'erc20') }} t2
ON t2.blockchain = 'ethereum'
AND b.stataToken = t2.contract_address
),

morpho_tokens AS(
SELECT DISTINCT
    a.metaMorpho AS erc4626_token,
    a.name AS erc4626_token_name,
    a.symbol AS erc4626_token_symbol,
    a.asset AS underlying_token,
    t.symbol AS underlying_token_symbol,
    18 AS decimals
FROM {{ source('metamorpho_factory_ethereum', 'MetaMorphoFactory_evt_CreateMetaMorpho') }} a
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND a.asset = t.contract_address

UNION 

SELECT DISTINCT
    a.metaMorpho AS erc4626_token,
    a.name AS erc4626_token_name,
    a.symbol AS erc4626_token_symbol,
    a.asset AS underlying_token,
    t.symbol AS underlying_token_symbol,
    18 AS decimals
FROM {{ source('metamorpho_factory_ethereum', 'MetaMorphoV1_1Factory_evt_CreateMetaMorpho') }} a
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND a.asset = t.contract_address
AND a.metaMorpho != 0xbeefc011e94f43b8b7b455ebab290c7ab4e216f1

UNION 

SELECT 
    erc4626_token,
    erc4626_token_name,
    erc4626_token_symbol,
    underlying_token,
    underlying_token_symbol,
    decimals
FROM (VALUES 
    (0xbeefc011e94f43b8b7b455ebab290c7ab4e216f1, 'Coinshift USDL', 'csUDL', 0xbdC7c08592Ee4aa51D06C27Ee23D5087D65aDbcD, 'USDL', 18)
    ) AS temp_table (erc4626_token, erc4626_token_name, erc4626_token_symbol, underlying_token, underlying_token_symbol, decimals)
)


SELECT 
    'ethereum' AS blockchain, 
    * 
FROM aave_tokens

UNION 

SELECT 
    'ethereum' AS blockchain, 
    * 
FROM morpho_tokens