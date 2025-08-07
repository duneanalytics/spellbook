{% set blockchain = 'opbnb' %}

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
    ('wbnb-wrapped-bnb', 'WBNB', 0x4200000000000000000000000000000000000006, 18)
    , ('usdt-tether', 'USDT', 0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3, 18)
    , ('bnb-binance-coin', 'BNB', 0x0000000000000000000000000000000000000000, 18)
    , ('eth-ethereum', 'ETH', 0xe7798f023fc62146e8aa1b36da45fb70855a77ea, 18)
) as temp (token_id, symbol, contract_address, decimals)
