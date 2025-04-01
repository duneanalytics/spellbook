{% set blockchain = 'boba' %}

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
    ('usdt-tether', 'USDT', 0x5DE1677344D3Cb0D7D465c10b72A8f60699C062d, 6)
    , ('usdc-usd-coin', 'USDC', 0x66a2A913e447d6b4BF33EFbec43aAeF87890FBbc, 6)
    , ('eth-ethereum', 'WETH', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 18)
    , ('boba-boba-network', 'BOBA', 0xa18bF3994C0Cc6E3b63ac420308E5383f53120D7, 18)
    , ('dai-dai', 'DAI', 0xf74195Bb8a5cf652411867c5C2C5b8C2a402be35, 18)
    , ('frax-frax', 'FRAX', 0x7562F525106F5d54E891e005867Bf489B5988CD9, 18)
    , ('bnb-binance-coin', 'BNB', 0x68ac1623ACf9eB9F88b65B5F229fE3e2c0d5789E, 18)
) as temp (token_id, symbol, contract_address, decimals)
