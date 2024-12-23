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
      (0x309C92261178fA0CF748A855e90Ae73FDb79EBc7, 'WETH', 18)
    , (0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38, 'WS', 18)
    , (0x29219dd400f2Bf60E5a23d13Be72B486D4038894, 'USDC.e', 6)
    , (0xe715cba7b5ccb33790cebff1436809d36cb17e57, 'EURC.e', 6)
    , (0xf2f9f482a395b4dd5b5b92173b7d62f3aff92884, 'DOG', 18)
    , (0x06341839dac9384bf96c6fc8df6983661a06356a, 'DOGE', 18)
    , (0x446649f0727621bdbb76644b1910be2163b62a11, 'SONIC', 18)
    , (0x9fdbc3f8abc05fa8f3ad3c17d2f806c1230c4564, 'GOGLZ', 18)
    , (0x2030170901a9d87f6bd0ca9b8ad130119c7e1173, 'BABYSONIC', 9)
    , (0xe5da20f15420ad15de0fa650600afc998bbe3955, 'stS', 18)
    , (0xd4a5c68a1ed1fc2bb06cba2d90d6adeee7503671, 'HOOPS', 18)

) as temp (contract_address, symbol, decimals)