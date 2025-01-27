{{
    config(
        schema = 'balancer_v3_sonic',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH wrapped_tokens AS(
SELECT 
    erc4626_token,
    erc4626_token_name,
    erc4626_token_symbol,
    underlying_token,
    underlying_token_symbol,
    decimals
FROM (VALUES 
     (0xA28d4dbcC90C849e3249D642f356D85296a12954, 'Wrapped Avalon Avalon SOLVBTCBBN', 'waSonicSOLVBTCBBN', 0xCC0966D8418d412c599A6421b760a847eB169A8c, 'SolvBTC.BBN', 18),
     (0xD31E89Ffb929b38bA60D1c7dBeB68c7712EAAb0a, 'Wrapped Avalon Avalon SOLVBTC', 'waSonicSOLVBTC', 0x541FD749419CA806a8bc7da8ac23D346f2dF8B77, 'SolvBTC', 18),
     (0x52Fc9E0a68b6a4C9b57b9D1d99fB71449A99DCd8, 'Silo Finance Borrowable SolvBTC.BBN Deposit', 'bSolvBTC.BBN-13', 0xCC0966D8418d412c599A6421b760a847eB169A8c, 'SolvBTC.BBN', 18),
     (0x87178fe8698C7eDa8aA207083C3d66aEa569aB98, 'Silo Finance Borrowable SolvBTC Deposit', 'bSolvBTC-13', 0x541FD749419CA806a8bc7da8ac23D346f2dF8B77, 'SolvBTC', 18),
     (0x7870ddFd5ACA4E977B2287e9A212bcbe8FC4135a, 'Beefy Wrapped SiloV2 USDC.e', 'BeefySiloV2USDC.e', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6)
    ) AS temp_table (erc4626_token, erc4626_token_name, erc4626_token_symbol, underlying_token, underlying_token_symbol, decimals)
)

SELECT DISTINCT
    'sonic' AS blockchain, 
    * 
FROM wrapped_tokens