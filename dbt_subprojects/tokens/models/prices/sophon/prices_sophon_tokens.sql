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
    -- ('soph-sophon', 'SOPH', 0x000000000000000000000000000000000000800A, 18)  -- Temporarily removed as no price data available yet
    ('eth-ethereum', 'ETH', 0x72af9F169B619D85A47Dfa8fefbCD39dE55c567D, 18)
) as temp (token_id, symbol, contract_address, decimals) 