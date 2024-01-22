{{ config(
        schema='prices_cosmos',
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
    , decimals
FROM
(
    VALUES 
        ('tia-celestia',null,'TIA',0x0000000000000000000000000000000000000000,8) --native token
) as temp (token_id, blockchain, symbol, contract_address, decimals)
