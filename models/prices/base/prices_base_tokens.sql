{{ config(
        schema='prices_base',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags = ['static', 'dunesql']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    ('weth-weth', 'base', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    ,('usdc-usd-coin', 'base', 'USDbC', 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA, 6)
    ,('dai-dai', 'base', 'DAI', 0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb, 18)
    ,('cbeth-coinbase-wrapped-staked-eth', 'base', 'cbETH', 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
