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
    , ('btcb-bitcoin-avalanche-bridged-btcb', 'BTC.b', 0xB0F70C0bD6FD87dbEb7C10dC692a2a6106817072, 8)
    , ('cusd-cap-usd', 'CUSD', 0xcCcc62962d17b8914c62D74FfB843d73B2a3cccC, 18)
    , ('ezeth-renzo-restaked-eth', 'ezETH', 0x09601A65e7de7BC8A19813D263dD9E98bFdC3c57, 18)
    , ('ezeth-renzo-restaked-eth', 'ezETH', 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110, 18)
    , ('pufeth-pufeth', 'pufETH', 0xD9A442856C234a39a81a089C06451EBAa4306a72, 18)
    , ('rseth-rseth', 'rsETH', 0xc3eACf0612346366Db554C991D7858716db09f58, 18)
    , ('rseth-rseth', 'rsETH', 0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7, 18)
    , ('susde-ethena-staked-usde', 'sUSDe', 0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2, 18)
    , ('susde-ethena-staked-usde', 'sUSDe', 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497, 18)
    , ('usde-ethena-usde', 'USDe', 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 18)
    , ('usde-ethena-usde', 'USDe', 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 18)
    , ('usdt0-usdt0', 'USDT0', 0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599, 8)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x32090fB1399a31cC095e6341a6353b7c09ba84FB, 8)
    , ('wrseth-wrapped-rseth', 'wrsETH', 0x4Fc44BE15e9B6E30C1E774E2C87A21D3E8b5403F, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x601aC63637933D88285A025C685AC4e9a92a98dA, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0, 18)
) as temp (token_id, symbol, contract_address, decimals)
