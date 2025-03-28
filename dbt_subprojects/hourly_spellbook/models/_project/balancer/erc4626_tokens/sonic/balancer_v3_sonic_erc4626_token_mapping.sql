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
     (0x7870ddFd5ACA4E977B2287e9A212bcbe8FC4135a, 'Beefy Wrapped SiloV2 USDC.e', 'BeefySiloV2USDC.e', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6),
     (0x016C306e103FbF48EC24810D078C65aD13c5f11B, 'Silo Finance Borrowable wS Deposit', 'bwS-25', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'wS', 18),
     (0x219656F33c58488D09d518BaDF50AA8CdCAcA2Aa, 'Silo Finance Borrowable WETH Deposit', 'bWETH-26', 0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 'WETH', 18),
     (0x6c49b18333a1135e9a376560c07e6d1fd0350eaf, 'Silo Finance Borrowable wS Deposit', 'bwS-28', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'wS', 18),
     (0x5954ce6671d97d24b782920ddcdbb4b1e63ab2de, 'Silo Finance Borrowable USDC.e Deposit', 'bUSDC.e-23', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6),
     (0x6646248971427B80ce531bdD793e2Eb859347E55, 'Wrapped Aave Sonic USDC', 'waSonUSDC', 0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6),
     (0x18B7B8695165290f2767BC63c36D3dFEa4C0F9bB, 'Wrapped Aave Sonic wS', 'waSonwS', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'wS', 18),
     (0xeB5e9B0ae5bb60274786C747A1A2A798c11271E0, 'Wrapped Aave Sonic WETH', 'waSonWETH', 0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 'WETH', 18),
     (0xda14a41dbda731f03a94cb722191639dd22b35b2, 'Silo Finance Borrowable frxUSD Deposit, SiloId: 37', 'bfrxUSD-37', 0x80Eede496655FB9047dd39d9f418d5483ED600df, 'frxUSD', 18),
     (0xa5cd24d9792f4f131f5976af935a505d19c8db2b, 'EVK Vault eWETH-1', 'eWETH-1', 0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 'WETH', 18),
     (0x0806af1762bdd85b167825ab1a64e31cf9497038, 'EVK Vault escETH-2', 'escETH-2', 0x3bcE5CB273F0F148010BbEa2470e7b5df84C7812, 'scETH', 18).
     (0x2A86Ebd12573f4633453899156DA81345AC1d57D, 'Vicuna Wrapped stS', 'waSonicSTS', 0xE5DA20F15420aD15DE0fa650600aFc998bbE3955, 'stS', 18).
     (0x6C2dadFfAB1714485aD87d1926f4c26E29a957b6, 'Vicuna Wrapped S', 'waSonicWS', 0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'wS', 18).
     (0x711a93a8bD6803aF0a6122F2dE18c1a6AB7CB29C, 'Vicuna Wrapped scUSD', 'waSonicSCUSD', 0xd3DCe716f3eF535C5Ff8d041c1A41C3bd89b97aE, 'scUSD', 6).
     (0xd7c9f62622dB85545731F0E4e5D4556aC8a19832, 'Vicuna Wrapped USDT', 'waSonicUSDt', 0x6047828dc181963ba44974801FF68e538dA5eaF9, 'USDT', 6).
     (0xef23FdCbd9b36Ed99A6C51CaA83Af549c36601CF, 'Vicuna Wrapped USDC.e', 'waSonicUSDC', 0x3bcE5CB20x29219dd400f2Bf60E5a23d13Be72B486D403889473F0F148010BbEa2470e7b5df84C7812, 'USDC.e', 6).
     (0x0A94e18bdbCcD048198806d7FF28A1B1D2590724, 'Silo Finance Borrowable scBTC Deposit, SiloId: 32', 'bscBTC-32', 0xBb30e76d9Bb2CC9631F7fC5Eb8e87B5Aff32bFbd, 'scBTC', 8).
    ) AS temp_table (erc4626_token, erc4626_token_name, erc4626_token_symbol, underlying_token, underlying_token_symbol, decimals)
)

SELECT DISTINCT
    'sonic' AS blockchain, 
    * 
FROM wrapped_tokens