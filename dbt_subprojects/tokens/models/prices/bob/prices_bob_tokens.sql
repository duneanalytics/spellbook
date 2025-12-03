{% set blockchain = 'bob' %}

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
    ('bob-bob-build-on-bitcoin', 'BOB', 0xB0BD54846a92b214C04A63B26AD7Dc5e19A60808, 18)
) as temp (token_id, symbol, contract_address, decimals)