{{ config(
        schema='prices_base',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags=['static', 'dunesql']
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

--    ('weth-weth', 'base', 'WETH', 0x4200000000000000000000000000000000000006, 18), --requested add to coinPaprika 2023-08-10
--    ('axl-axelar', 'base', 'AXL', 0x467719aD09025FcC6cF6F8311755809d45a5E5f3, 6), --requested add to coinPaprika 2023-08-10
    ('bald-bald', 'base', 'BALD', 0x27D2DECb4bFC9C76F0309b8E88dec3a601Fe25a8, 18),
    ('usdbc-usd-base-coin', 'base', 'USDbC', 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA, 6),
    ('usdc-usd-coin', 'base', 'USDC', 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913, 6),
    ('weth-weth', 'base', 'WETH', 0x4200000000000000000000000000000000000006, 18),
    ('dai-dai', 'base', 'DAI', 0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb, 18),
    ('cbeth-coinbase-wrapped-staked-eth', 'base', 'cbETH', 0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22, 18)
    

 
   
) as temp (token_id, blockchain, symbol, contract_address, decimals)
