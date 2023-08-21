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
    ('bald-bald', 'base', 'BALD', 0x27D2DECb4bFC9C76F0309b8E88dec3a601Fe25a8, 18)
 
   
) as temp (token_id, blockchain, symbol, contract_address, decimals)
