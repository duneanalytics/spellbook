{% set blockchain = 'aptos' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

-- ci-stamp: 1
select
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , to_utf8(contract_address) as contract_address
    , contract_address as contract_address_native
    , cast(decimals as integer) as decimals
from
(
    values
    ('usdc-usd-coin', 'USDC', '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC', 6)
    , ('usdt-tether', 'USDT', '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT', 6)
    , ('weth-weth', 'WETH', '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WETH', 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', '0xae478ff7d83ed072dbc5e264250e67ef58f57c99d89b447efd8a0a2e8b2be76e::coin::T', 8)
    , ('thl-thala', 'THL', '0x07fd500c11216f0fe3095d0c4b8aa4d64a4e2e04f83758462f2b127255643615::thl_coin::THL', 8)
) as temp (token_id, symbol, contract_address, decimals)
