{% set blockchain = 'xlayer' %}

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
    -- placeholder rows, add actual tokens as needed
    ('x-layer', 'XL', 0x0000000000000000000000000000000000000000, 18) -- dummy token for now
) as temp (token_id, symbol, contract_address, decimals)
