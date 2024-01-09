{{ config(
        schema='prices_cardano',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT 
    token_id
    , blockchain
    , symbol
    , contract_address
    , CAST(decimals as int) as decimals
FROM
(
    VALUES
    ('snek-snek-crd', 'cardano', 'SNEK', '279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f', null)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
