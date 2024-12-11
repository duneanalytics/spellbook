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
    b.staticaToken AS erc4626_token,
    a.staticaTokenName AS erc4626_token_name,
    a.staticaTokenSymbol AS erc4626_token_symbol,
    b.underlying AS underlying_token,
    t.symbol AS underlying_token_symbol,
    t.decimals AS token_decimals
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
    erc4626_token,
    erc4626_token_name,
    erc4626_token_symbol,
    underlying_token,
    underlying_token_symbol,
    token_decimals
FROM (VALUES 
     (0xd4fa2d31b7968e448877f69a96de69f5de8cd23e, 'Static Aave Ethereum USDC', 'WaEthUSDC', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USDC', 6),
     (0x773CDA0CADe2A3d86E6D4e30699d40bB95174ff2, 'Static Aave Ethereum USDT', 'WaEthUSDT', 0xdac17f958d2ee523a2206206994597c13d831ec7, 'USDT', 6),
     (0x0bfc9d54fc184518a81162f8fb99c2eaca081202, 'Static Aave Ethereum WETH', 'WaEthWETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'WETH', 18),
     (0x51350d88c1bd32Cc6A79368c9Fb70373Fb71F375, 'Static Aave Ethereum Lido WETH', 'waEthLidoWETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'WETH', 18),
     (0x51350d88c1bd32Cc6A79368c9Fb70373Fb71F375, 'Static Aave Ethereum Lido wstETH', 'waEthLidowstETH', 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 'wstETH', 18)
    ) AS temp_table (erc4626_token, erc4626_token_name, erc4626_token_symbol, underlying_token, underlying_token_symbol, token_decimals)
),

morpho_tokens AS(
SELECT DISTINCT
    a.metaMorpho AS erc4626_token,
    a.name AS erc4626_token_name,
    a.symbol AS erc4626_token_symbol,
    a.asset AS underlying_token,
    t.symbol AS underlying_token_symbol,
    18 AS token_decimals
FROM {{ source('metamorpho_factory_ethereum', 'MetaMorphoFactory_evt_CreateMetaMorpho') }} a
JOIN {{ source('tokens', 'erc20') }} t
ON t.blockchain = 'ethereum'
AND a.asset = t.contract_address
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