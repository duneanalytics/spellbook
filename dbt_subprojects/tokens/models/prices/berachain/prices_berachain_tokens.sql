{% set blockchain = 'berachain' %}

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
    ('usde-ethena-usde', 'USDe', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 18)
    , ('weeth-wrapped-eeth', 'weETH', 0x7dcc39b4d1c53cb31e1abc0e358b43987fef80f7, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecac9c5f704e954931349da37f60e39f515c11c1, 8)
) as temp (token_id, symbol, contract_address, decimals) 