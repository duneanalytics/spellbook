{% set blockchain = 'abstract' %}

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
    ('weth-weth', 'WETH', 0x3439153EB7AF838Ad19d56E1571FBD09333C2809, 18)
    , ('usdc-usd-coin', 'USDC.e', 0x84a71ccd554cc1b02749b35d22f684cc8ec987e1, 6)
    , ('pengu-pudgy-penguins', 'PENGU', 0x9ebe3a824ca958e4b3da772d2065518f009cba62, 18)
) as temp (token_id, symbol, contract_address, decimals) 