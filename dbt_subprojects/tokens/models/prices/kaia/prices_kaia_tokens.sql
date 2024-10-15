{% set blockchain = 'kaia' %}

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
    ('usdt-tether', 'oUSDT', 0xcee8faf64bb97a73bb51e115aa89c17ffa8dd167, 6)
    , ('eth-ethereum', 'oETH', 0x34d21b1e550d73cee41151c77f3c73359527a396, 18)
    , ('usdc-usd-coin', 'oUSDC', 0x754288077d0ff82af7a5317c7cb8c444d421d103, 6)
) as temp (token_id, symbol, contract_address, decimals)