{% set blockchain = 'sophon' %}

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
    ('eth-ethereum', 'ETH', 0x72af9F169B619D85A47Dfa8fefbCD39dE55c567D, 18)
    , ('usdt-tether', 'USDT', 0x6386da73545ae4e2b2e0393688fa8b65bb9a7169, 6)
    , ('usdc-usd-coin', 'USDC', 0x9Aa0F72392B5784Ad86c6f3E899bCc053D00Db4F, 6)
) as temp (token_id, symbol, contract_address, decimals) 