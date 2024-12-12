{{
    config(
        schema = 'balancer_v3_gnosis',
        alias = 'erc4626_tokens_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(

SELECT 
    erc4626_token,
    erc4626_token_name,
    erc4626_token_symbol,
    underlying_token,
    underlying_token_symbol,
    decimals
FROM (VALUES 
     (0x7c16F0185A26Db0AE7a9377f23BC18ea7ce5d644, 'Static Aave Gnosis GNO', 'WaGnoGNO', 0x9C58BAcC331c9aa871AFD802DB6379a98e80CEdb, 'GNO', 18),
     (0x773CDA0CADe2A3d86E6D4e30699d40bB95174ff2, 'Static Aave Gnosis wstETH', 'WaGnowstETH', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6, 'wstETH', 18),
     (0x57f664882F762FA37903FC864e2B633D384B411A, 'Static Aave Gnosis WETH', 'WaGnoWETH', 0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1, 'WETH', 18),
     (0x51350d88c1bd32Cc6A79368c9Fb70373Fb71F375, 'Static Aave Gnosis USDC', 'waGnoUSDCe', 0x2a22f9c3b484c3629090FeED35F17Ff8F88f76F0, 'USDC', 6)
    ) AS temp_table (erc4626_token, erc4626_token_name, erc4626_token_symbol, underlying_token, underlying_token_symbol, decimals)
)

SELECT 
    'gnosis' AS blockchain, 
    * 
FROM aave_tokens