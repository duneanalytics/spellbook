{% set blockchain = 'hemi' %}

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
    , ('usdc-usd-coin', 'USDC', 0xad11a8BEb98bbf61dbb1aa0F6d6F2ECD87b35afA, 6)
    , ('usdt-tether', 'USDT', 0xbB0D083fb1be0A9f6157ec484b6C79E0A4e31C2e, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x03C7054BCB39f7b2e5B2c7AcB37583e32D70Cfa3, 8)
) as temp (token_id, symbol, contract_address, decimals)
