{% set blockchain = 'megaeth' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('btcb-bitcoin-avalanche-bridged-btcb', 'BTC.b', 0xb0f70c0bd6fd87dbeb7c10dc692a2a6106817072, 8)
    , ('cusd-cap-usd', 'CUSD', 0xcccc62962d17b8914c62d74ffb843d73b2a3cccc, 18)
    , ('ezeth-renzo-restaked-eth', 'ezETH', 0x09601a65e7de7bc8a19813d263dd9e98bfdc3c57, 18)
    , ('ezeth-renzo-restaked-eth', 'ezETH', 0xbf5495efe5db9ce00f80364c8b423567e58d2110, 18)
    , ('pufeth-pufeth', 'pufETH', 0xd9a442856c234a39a81a089c06451ebaa4306a72, 18)
    , ('rseth-rseth', 'rsETH', 0xc3eacf0612346366db554c991d7858716db09f58, 18)
    , ('stcusd-staked-cap-usd', 'rsETH', 0xa1290d69c65a6fe4df752f95823fae25cb99e5a7, 18)
    , ('stcusd-staked-cap-usd', 'STCUSD', 0x88887be419578051ff9f4eb6c858a951921d8888, 18)
    , ('susde-ethena-staked-usde', 'sUSDe', 0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2, 18)
    , ('susde-ethena-staked-usde', 'sUSDe', 0x9d39a5de30e57443bff2a8307a4256c8797a3497, 18)
    , ('usde-ethena-usde', 'USDe', 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 18)
    , ('usde-ethena-usde', 'USDe', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 18)
    , ('usdt0-usdt0', 'USDT0', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 8)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x32090fb1399a31cc095e6341a6353b7c09ba84fb, 8)
    , ('wrseth-wrapped-rseth', 'wrsETH', 0x4fc44be15e9b6e30c1e774e2c87a21d3e8b5403f, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'WETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x601ac63637933d88285a025c685ac4e9a92a98da, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 18)
) as temp (token_id, symbol, contract_address, decimals)
