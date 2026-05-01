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
    , ('rseth-rseth', 'rsETH', 0xc3eACf0612346366Db554C991D7858716db09f58, 18)
    , ('susde-ethena-staked-usde', 'sUSDe', 0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2, 18)
    , ('usde-ethena-usde', 'USDe', 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 18)
    , ('usdt0-usdt0', 'USDT0', 0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb, 6)
    --, ('wbtc-wrapped-bitcoin', 'WBTC', 0x32090fB1399a31cC095e6341a6353b7c09ba84FB, 8) seems like wrong address, excluding for now
    , ('wrseth-wrapped-rseth', 'wrsETH', 0x4Fc44BE15e9B6E30C1E774E2C87A21D3E8b5403F, 18)
    , ('usdc-usd-coin', 'MegaUSD', 0xfafddbb3fc7688494971a79cc65dca3ef82079e7, 18) -- TODO: replace token_id with USDm once integrated to coinpaprika
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x601aC63637933D88285A025C685AC4e9a92a98dA, 18)
) as temp (token_id, symbol, contract_address, decimals)
