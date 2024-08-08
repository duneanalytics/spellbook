{% set blockchain = 'nova' %}

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
    ('arb-arbitrum', 'ARB', 0xf823C3cD3CeBE0a1fA952ba88Dc9EEf8e0Bf46AD, 18)
    , ('usdc-usd-coin', 'USDC', 0x750ba8b76187092B0D1E87E28daaf484d1b5273b, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x1d05e4e72cD994cdF976181CfB0707345763564d, 8)
    , ('weth-weth', 'WETH', 0x722E8BdD2ce80A4422E880164f2079488e115365, 18)
    , ('dai-dai', 'DAI', 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1, 18)
) as temp (token_id, symbol, contract_address, decimals)