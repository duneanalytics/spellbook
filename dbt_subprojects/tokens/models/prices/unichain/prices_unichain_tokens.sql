{% set blockchain = 'unichain' %}

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
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('usdc-usd-coin', 'USDC', 0x078d782b760474a361dda0af3839290b0ef57ad6, 6)
    , ('uni-uniswap', 'UNI', 0x8f187aa05619a017077f5308904739877ce9ea21, 18)
    , ('dai-dai', 'DAI', 0x20cab320a855b39f724131c69424240519573f81, 18)
) as temp (token_id, symbol, contract_address, decimals) 