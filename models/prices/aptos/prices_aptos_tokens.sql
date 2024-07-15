{{ config(
        schema='prices_aptos',
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
    , from_base58(contract_address) as contract_address
    , decimals
FROM
(
    VALUES
    ('apt-aptos','aptos','APT','0x1::aptos_coin::AptosCoin',8)
    
) as temp (token_id, blockchain, symbol, contract_address, decimals)
