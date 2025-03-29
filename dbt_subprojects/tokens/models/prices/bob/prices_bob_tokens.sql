{{ config(
        schema='prices_bob',
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

    ('dai-dai', 'bob', 'DAI', 0x6c851f501a3f24e29a8e39a29591cddf09369080, 18),
    ('dllr-sovryndollar', 'bob', 'DLLR', 0xf3107eec1e6f067552c035fd87199e1a5169cb20, 18),
    ('eth-ethereum', 'bob', 'ETH', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 18),
    ('frax-frax', 'bob', 'FRAX', 0xc4a20a608616f18aa631316eeda9fb62d089361e, 18),
    ('fxs-frax-share', 'bob', 'FXS', 0x15e35b19ad29c512103eaabb55154ef0ee6ca661, 18),
    ('reth-rocket-pool-eth', 'bob', 'rETH', 0xb5686c4f60904ec2bda6277d6fe1f7caa8d1b41a, 18),
    ('sfrax-frax-finance', 'bob', 'sFRAX', 0xb7eae04b995b3b365040dee99795112add43afa0, 18),
    ('sov-sovryn', 'bob', 'SOV', 0xba20a5e63eeefffa6fd365e7e540628f8fc61474, 18),
    ('stone-stakestone-ether', 'bob', 'STONE', 0x96147a9ae9a42d7da551fd2322ca15b71032f342, 18),
    ('tbtc-tbtc', 'bob', 'tBTC', 0xbba2ef945d523c4e2608c9e1214c2cc64d4fc2e2, 18),
    ('trb-tellor-tributes', 'bob', 'TRB', 0x665060707c3ea3c31b3eabad7f409072446e1d50, 18),
    ('usdc-usd-coin', 'bob', 'USDC', 0xe75d0fb2c24a55ca1e3f96781a2bcc7bdba058f0, 6),
    ('usdt-tether', 'bob', 'USDT', 0x05d032ac25d322df992303dca074ee7392c117b9, 6),
    ('wbtc-wrapped-bitcoin', 'bob', 'WBTC', 0x03c7054bcb39f7b2e5b2c7acb37583e32d70cfa3, 8),
    ('wsteth-wrapped-liquid-staked-ether-20', 'bob', 'wstETH', 0x85008ae6198bc91ac0735cb5497cf125ddaac528, 18),
    ('lbtc-lombard-staked-btc', 'bob','LBTC', 0xA45d4121b3D47719FF57a947A9d961539Ba33204, 8),
    ('wbtc-wrapped-bitcoin', 'bob', 'HybridBTC.pendle', 0x9998e05030Aee3Af9AD3df35A34F5C51e1628779, 8)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
