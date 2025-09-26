{% set blockchain = 'plasma' %}

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
    ('xpl-plasma', 'WXPL', 0x6100E367285b01F48D07953803A2d8dCA5D19873, 18)
    , ('usdt-tether', 'USDT0', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb, 6)
    , ('weth-weth', 'WETH', 0x9895d81bb462a195b4922ed7de0e3acd007c32cb, 18)
    , ('xaut-tether-gold', 'XAUT0', 0x1B64B9025EEbb9A6239575dF9Ea4b9Ac46D4d193, 6)
    , ('usde-ethena-usde', 'USDe', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 18)
    , ('dai-dai', 'USDai', 0x0A1a1A107E45b7Ced86833863f482BC5f4ed82EF, 18)
) as temp (token_id, symbol, contract_address, decimals) 