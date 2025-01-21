{{
    config(
        schema = 'tokens_sonic'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
      (0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 'WETH', 18)
    , (0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'wS', 18)
    , (0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6)
    , (0xe715cba7b5ccb33790cebff1436809d36cb17e57, 'EURC.e', 6)
    , (0xf2f9f482a395b4dd5b5b92173b7d62f3aff92884, 'DOG', 18)
    , (0x06341839dac9384bf96c6fc8df6983661a06356a, 'DOGE', 18)
    , (0x446649f0727621bdbb76644b1910be2163b62a11, 'SONIC', 18)
    , (0x9fdbc3f8abc05fa8f3ad3c17d2f806c1230c4564, 'GOGLZ', 18)
    , (0x2030170901a9d87f6bd0ca9b8ad130119c7e1173, 'BABYSONIC', 9)
    , (0xe5da20f15420ad15de0fa650600afc998bbe3955, 'stS', 18)
    , (0xd4a5c68a1ed1fc2bb06cba2d90d6adeee7503671, 'HOOPS', 18)
    , (0x1e5fe95fb90ac0530f581c617272cd0864626795, 'BEETS', 18)
    , (0xe17be928a08d66156ec242a68c4929b07cf14ccc, 'SCARTEL', 18)
    , (0x309c92261178fa0cf748a855e90ae73fdb79ebc7, 'WETH', 18)
    , (0x342781fd3d3f73bdb54b2dedd925f2ad81b48590, 'SPOOH', 18)
    , (0xb5a10bd15af95381d830f71e85328f2d1d823de7, 'SPUB', 18)
    , (0x71d634647a1aa323c1a0e6d9548ddaa6eb750e6e, 'SHOB', 18)
    , (0x93cd403693da40d27188714a872943f717e3c543, 'SBEER', 18)
    , (0x2d10a2e2d680564a942e98ee43e35553db990807, 'STEQILA', 18)
    , (0x3854fd4712eb3a5aa51c949c29cace84f7ed2efc, 'Shilly', 18)   
    , (0x2d0e0814e62d80056181f5cd932274405966e4f0, 'BEETS', 18)
    , (0xd3DCe716f3eF535C5Ff8d041c1A41C3bd89b97aE, 'scUSD', 6)   
    , (0x3bce5cb273f0f148010bbea2470e7b5df84c7812, 'scETH', 18)    
    , (0xddf26b42c1d903de8962d3f79a74a501420d5f19, 'EQUAL', 18)
    , (0x313636D4f23859142b523a7965B76F6e3965Af64, 'SonicSwap', 18)
    , (0x4eec869d847a6d13b0f6d1733c5dec0d1e741b4f, 'INDI', 18)
    , (0x4D85bA8c3918359c78Ed09581E5bc7578ba932ba, 'stkscUSD', 6)
    , (0x455d5f11Fea33A8fa9D3e285930b478B6bF85265, 'stkscETH', 18)
    , (0x541FD749419CA806a8bc7da8ac23D346f2dF8B77, 'SolvBTC', 18)
    , (0xCC0966D8418d412c599A6421b760a847eB169A8c, 'solvBTC.bbn', 18)
) as temp (contract_address, symbol, decimals)
