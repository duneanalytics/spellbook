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
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
) as temp (token_id, symbol, contract_address, decimals) 