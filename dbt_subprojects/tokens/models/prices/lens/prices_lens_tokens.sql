{% set blockchain = 'lens' %}

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
    ('gho-gho', 'GHO', 0x000000000000000000000000000000000000800A, 18)
    , ('gho-gho', 'WGHO', 0x6bDc36E20D267Ff0dd6097799f82e78907105e2F, 18)
) as temp (token_id, symbol, contract_address, decimals) 