{{ config(
        schema='prices_base',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags=['static', 'dunesql']
        )
}}
SELECT 
    token_id
    , 'zksync' AS blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth', 'ETH', 0x000000000000000000000000000000000000800a, 18)
    , ('weth-weth', 'WETH', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 18)
    , ('usdce-usd-coine', 'USDC.e', 0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4, 18)
    , ('busd-binance-usd', 'BUSD', 0x2039bb4116b4efc145ec4f0e2ea75012d6c0f181, 18)
    , ('usdte-tether-usde', 'USDT.e', 0x493257fd37edb34451f62edf8d2a0c418852ba4c, 6)
    , ('mute-mute', 'MUTE', 0x0e97c7a0f8b2c9885c8ac9fc6136e829cbc21d42, 18)
    , ('mav-maverick-token', 'MAV', 0x787c09494ec8bcb24dcaf8659e7d5d69979ee508, 18)
    , ('zkusd-zkusd', 'zkUSD', 0xfc7e56298657b002b3e656400e746b7212912757, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0xbbeb516fb02a01611cbbe0453fe3c580d7281011, 8)
    , ('izi-izumi-finance', 'iZi', 0x16a9494e257703797d747540f01683952547ee5b, 18)
    , ('lusd-liquity-usd', 'LUSD', 0x503234f203fc7eb888eec8513210612a43cf6115, 18)
    , ('nusd-neutral-dollar', 'nUSD', 0x1181d7be04d80a8ae096641ee1a87f7d557c6aeb, 8)
    , ('combo-furucombo', 'COMBO', 0xc2B13Bb90E33F1E191b8aA8F44Ce11534D5698E3, 18)
    , ('perp-perpetual-protocol', 'PERP', 0x42c1c56be243c250AB24D2ecdcC77F9cCAa59601, 18)
    , ('dvf-rhinofi', 'DVF', 0xBbD1bA24d589C319C86519646817F2F153c9B716, 18)
    , ('woo-wootrade', 'WOO', 0x9E22D758629761FC5708c171d06c2faBB60B5159, 18)
    , ('deri-deri-protocol', 'DERI', 0x140D5bc5b62d6cB492B1A475127F50d531023803, 18)
    , ('dextf-dextf', 'DEXTF', 0x9929bCAC4417A21d7e6FC86F6Dae1Cc7f27A2e41, 18)
    , ('govi-govi', 'GOVI', 0xD63eF5e9C628c8a0E8984CDfb7444AEE44B09044, 18)
    , ('1inch-1inch', '1INCH', 0x3f0B8B206A7FBdB3ecFc08c9407CA83F5aB1Ce59, 18)
    , ('sis-symbiosis-finance', 'SIS', 0xdd9f72afED3631a6C85b5369D84875e6c42f1827, 18)
    , ('lqty-liquity', 'LQTY', 0xf755cF4f0887279a8BCBE5E39eE062a5B7188401, 18)
    , ('pepe-pepe', 'PEPE', 0xFD282F16a64c6D304aC05d1A58Da15bed0467c71, 18)
    , ('reth-rocket-pool-eth', 'rETH', 0x32Fd44bB869620C0EF993754c8a00Be67C464806, 18)
    , ('rpl-rocket-pool', 'RPL', 0x32Fd44bB869620C0EF993754c8a00Be67C464806, 18)
    , ('ufi-purefi', 'UFI', 0xa0C1BC64364d39c7239bd0118b70039dBe5BbdAE, 18)
    , ('cbeth-coinbase-wrapped-staked-eth', 'cbETH', 0x75Af292c1c9a37b3EA2E6041168B4E48875b9ED5, 18)
    , ('ibex-impermax', 'IBEX', 0xbe9f8C0d6f0Fd7e46CDaCCA340747EA2f247991D, 18)
    , ('lsd-lsdx-finance', 'LSD', 0x458A2E32eAbc7626187E6b75f29D7030a5202bD4, 18)
    , ('knc-kyber-network', 'KNC', 0x6ee46Cb7cD2f15Ee1ec9534cf29a5b51C83283e6, 18)
    , ('bel-bella-protocol', 'BEL', 0xB83CFB285fc8D936E8647FA9b1cC641dBAae92D9, 18)
    , ('zz-zigzag', 'ZZ', 0x1ab721f531Cab4c87d536bE8B985EAFCE17f0184, 18)
    , ('bitcoin-harrypotterobamasonic10inu-eth', 'BITCOIN', 0x26b7F317C440E57db2fb4b377A3f1b3BBF5463C7, 18)
    , ('byn-nbx', 'BYN', 0x2d850F34E957BA3dcbEe47fc2c79ff78044fB12e, 18)
    , ('wagmi5-wag', 'WAGMI', 0xD7C6210f3d6011D6B1BdDfA60440fe763340Df4c, 18)
    , ('pool-pooltogether', 'POOL', 0x97003aC71CC4a096E06C73e753d9b84f0039A064, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)