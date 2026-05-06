{% set blockchain = 'tezos_evm' %}

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
    ('xtz-tezos', 'WXTZ', 0xc9B53AB2679f573e480d01e0f49e2B5CFB7a3EAb, 18)
    , ('usdc-usd-coin', 'USDC', 0x796Ea11Fa2dD751eD01b53C372fFDB4AAa8f00F9, 6)
    , ('usdt-tether', 'USDT', 0x2C03058C8AFC06713be23e58D2febC8337dbfE6A, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0xbFc94CD2B1E55999Cfc7347a9313e88702B83d0F, 8)
    , ('weth-weth', 'WETH', 0xfc24f770F94edBca6D6f885E12d4317320BcB401, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecAc9C5F704e954931349Da37F60E39f515c11c1, 8)
    , ('ustbl-ustbl-tokenized-us-treasury-bill', 'USTBL', 0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 5)
    , ('usdtz-usdtz', 'USDtz', 0x5b456255fc3389a1ed284a74be0d1f38734d3d6a, 6)
    , ('vnxau-vnx-gold', 'VNXAU', 0x93F5475da60143C50e8bE3fED10c143B0CF8b9E9, 18)
) as temp (token_id, symbol, contract_address, decimals)
