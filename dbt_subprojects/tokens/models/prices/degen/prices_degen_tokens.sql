{% set blockchain = 'degen' %}

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
    ('degen-degen-base', 'DEGEN', 0x0000000000000000000000000000000000000000, 18)
    , ('degen-degen-base', 'WDEGEN', 0xEb54dACB4C2ccb64F8074eceEa33b5eBb38E5387, 18)
) as temp (token_id, symbol, contract_address, decimals)
