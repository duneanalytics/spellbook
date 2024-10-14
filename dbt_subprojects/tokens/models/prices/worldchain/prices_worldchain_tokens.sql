{% set blockchain = 'worldchain' %}

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
    ('wld-worldcoin', 'WLD', 0x2cFc85d8E48F8EAB294be644d9E25C3030863003, 18)
    , ('usdc-usd-coin', 'USDC.e', 0x79A02482A880bCE3F13e09Da970dC34db4CD24d1, 6)
) as temp (token_id, symbol, contract_address, decimals)