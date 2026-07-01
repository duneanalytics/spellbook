{% set blockchain = 'robinhood' %}

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
    ('weth-weth', 'WETH', 0x0bd7D308F8E1639FaB988DF18A8011f41eACad73, 18)
    , ('usdg-global-dollar', 'USDG', 0x5fc5360D0400a0Fd4F2Af552ADd042d716f1D168, 6)
) as temp (token_id, symbol, contract_address, decimals)
