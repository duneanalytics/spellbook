{% set blockchain = 'megaeth' %}

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
    ('weth-weth', 'WETH', 0x4eB2Bd7beE16F38B1F4a0A5796Fffd028b6040e9, 18)
) as temp (token_id, symbol, contract_address, decimals)
