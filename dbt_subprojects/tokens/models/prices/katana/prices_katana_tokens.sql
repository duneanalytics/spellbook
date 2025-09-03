{% set blockchain = 'katana' %}

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
    ('usol-wrapped-solana-universal', 'uSOL', 0x9b8df6e244526ab5f6e6400d331db28c8fdddb55, 18)
    , ('weth-weth', 'vbETH', 0xEE7D8BCFb72bC1880D0Cf19822eB0A2e6577aB62, 18)
    , ('usdc-usd-coin', 'vbUSDC', 0x203A662b0BD271A6ed5a60EdFbd04bFce608FD36, 6)
    , ('usdt-tether', 'vbUSDT', 0x2DCa96907fde857dd3D816880A0df407eeB2D2F2, 6)
    , ('wbtc-wrapped-bitcoin', 'vbBTC', 0x0913DA6Da4b42f538B445599b46Bb4622342Cf52, 8)
    , ('usds-usds', 'vbUSDS', 0x62D6A123E8D19d06d68cf0d2294F9A3A0362c6b3, 18)
) as temp (token_id, symbol, contract_address, decimals)
