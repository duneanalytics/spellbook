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
    ('gho-gho', 'WGHO', 0x6bDc36E20D267Ff0dd6097799f82e78907105e2F, 18)
    , ('eth-ethereum', 'WETH', 0xE5ecd226b3032910CEaa43ba92EE8232f8237553, 18)
    , ('usdc-usd-coin', 'USDC', 0x88F08E304EC4f90D644Cec3Fb69b8aD414acf884, 6) 
    , ('bonsai-bonsai-token', 'BONSAI', 0xB0588f9A9cADe7CD5f194a5fe77AcD6A58250f82, 18)
) as temp (token_id, symbol, contract_address, decimals)