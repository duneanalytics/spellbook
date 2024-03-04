{{ config(
        schema='prices_zksync',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    ('busd-binance-usd', 'zksync', 'BUSD', 0x2039bb4116b4efc145ec4f0e2ea75012d6c0f181, 18),
    ('eth-ethereum', 'zksync', 'ETH', 0x000000000000000000000000000000000000800a, 18),
    ('izi-izumi-finance', 'zksync', 'iZi', 0x16a9494e257703797d747540f01683952547ee5b, 18),
    ('mav-maverick-token', 'zksync', 'MAV', 0x787c09494ec8bcb24dcaf8659e7d5d69979ee508, 18),
    ('mute-mute', 'zksync', 'MUTE', 0x0e97c7a0f8b2c9885c8ac9fc6136e829cbc21d42, 18),
    ('lusd-liquity-usd', 'zksync', 'LUSD', 0x503234f203fc7eb888eec8513210612a43cf6115, 18),
    ('usdc-usd-coin', 'zksync', 'USDC.e', 0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4, 6),
    ('usdt-tether', 'zksync', 'USDT.e', 0x493257fd37edb34451f62edf8d2a0c418852ba4c, 6),
    ('wbtc-wrapped-bitcoin', 'zksync', 'WBTC', 0xbbeb516fb02a01611cbbe0453fe3c580d7281011, 8),
    ('weth-weth', 'zksync', 'WETH', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 18),
    ('wsteth-wrapped-liquid-staked-ether-20', 'zksync', 'wstETH', 0x703b52F2b28fEbcB60E1372858AF5b18849FE867, 18),
    ('hold-holdstation', 'zksync', 'HOLD', 0xed4040fd47629e7c8fbb7da76bb50b3e7695f0f2, 18),
    ('dai-dai', 'zksync', 'DAI', 0x4B9eb6c0b6ea15176BBF62841C6B2A8a398cb656, 18),
    ('link-chainlink', 'zksync', 'LINK', 0x082faDe8b84B18C441d506e1D3a43a387Cc59D20, 18),
    ('matic-polygon', 'zksync', 'MATIC', 0x28a487240e4D45CfF4A2980D334CC933B7483842, 18),
    ('ldo-lido-dao', 'zksync', 'LDO', 0x44B809fb33C5f6B1737a3283adaeCa4aF27A8Ec9, 18),
    ('inj-injective-protocol', 'zksync', 'INJ', 0xc17F02Bd7f44A840A39AB41cf89AA831fE3f8a18, 18),
    ('cro-cryptocom-chain', 'zksync', 'CRO', 0x0bcCa089F12AE9e78f5060E1d6fbbe1C827c5990, 8),
    ('arb-arbitrum', 'zksync', 'ARB', 0xd5428B08b604727c43ba5a37EeD25a289978d081, 18),
    ('reth-rocket-pool-eth', 'zksync', 'rETH', 0x32Fd44bB869620C0EF993754c8a00Be67C464806, 18),
    ('woo-wootrade', 'zksync', 'WOO', 0x9E22D758629761FC5708c171d06c2faBB60B5159, 18),
    ('gala-gala', 'zksync', 'GALA', 0x5865F29a5A9D5BA8950C13506b014F1377F0b89f, 8),
    ('crv-curve-dao-token', 'zksync', 'CRV', 0x5945932099f124194452a4c62d34bB37f16183B2, 18),
    ('rpl-rocket-pool', 'zksync', 'RPL', 0x1CF8553Da5a75C20cdC33532cb19Ef7E3bFFf5BC, 18),
    ('knc-kyber-network', 'zksync', 'KNC', 0x6ee46Cb7cD2f15Ee1ec9534cf29a5b51C83283e6, 18),
    ('wagmi5-wagmi', 'zksync', 'WAGMI', 0x3613AD277DF1d5935D41400A181Aa9ec1DC2Dc9e, 18),
    ('sis-symbiosis-finance', 'zksync', 'SIS', 0xdd9f72afED3631a6C85b5369D84875e6c42f1827, 18),
    ('cake-pancakeswap', 'zksync', 'CAKE', 0x3A287a06c66f9E95a56327185cA2BDF5f031cEcD, 18),
    ('dextf-dextf', 'zksync', 'DEXTF', 0x9929bCAC4417A21d7e6FC86F6Dae1Cc7f27A2e41, 18),
    ('grai-gravita-protocol', 'zksync', 'GRAI', 0x5FC44E95eaa48F9eB84Be17bd3aC66B6A82Af709, 18),
    ('zz-zigzag', 'zksync', 'ZZ', 0x1ab721f531Cab4c87d536bE8B985EAFCE17f0184, 18),
    ('wefi-wefi-finance', 'zksync', 'WEFI', 0x81E7186947fb59AAAAEb476a47daAc60680cbbaF, 18),
    ('launch-superlauncher', 'zksync', 'LAUNCH', 0xF6D9a093A1C69a152d87e269A7d909E9D76B1815, 18),
    ('uni-uniswap', 'zksync', 'UNI', 0x1C6f53185061D7cC387E481c350aD00C2C876f3E, 18),
    ('shib-shiba-inu', 'zksync', 'SHIB', 0x5B09802d62d213c4503B4b1Ef5F727ef62c9F4eF, 18),
    ('govi-govi', 'zksync', 'GOVI', 0xD63eF5e9C628c8a0E8984CDfb7444AEE44B09044, 18),
    ('zkpepe1-zkpepe', 'zksync', 'ZKPEPE', 0x7d54a311d56957fa3c9a3e397ca9dc6061113ab3, 18),
    ('zat-zkapes-token', 'zksync', 'ZAT', 0x47EF4A5641992A72CFd57b9406c9D9cefEE8e0C4, 18),
    ('kat-karat', 'zksync', 'KAT', 0xCDb7D260c107499C80B4b748e8331c64595972a1, 18),
    ('zkid-zksync-id', 'zksync', 'ZKID', 0x2141d7fe06A1d69c016fC638bA75b6Ef92Fa1435, 18)

) as temp (token_id, blockchain, symbol, contract_address, decimals)
