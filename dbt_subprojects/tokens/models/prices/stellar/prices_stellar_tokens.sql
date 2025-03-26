{{ config(
        schema='prices_stellar',
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
    , CAST(contract_address as VARBINARY) as contract_address
    , CAST(null as int) as decimals
FROM
(
    VALUES
    ('velo-velo', 'stellar', 'VELO', 'VELO-GD7TJVZZAUDCXL5WBSXYAHEK4FJ7M5TRLOT6E3NKKCT3DDTA2WFESQDR-1'),
    ('aqua-aquarius', 'stellar', 'AQUA', 'GBNZILSTVQZ4R7IKQDGHYGY2QXL5QOFJYQMXPKWRRM5PAV7Y4M67AQUA'),
    ('afr-afreum', 'stellar', 'AFREUM', 'AFR-GBX6YI45VU7WNAAKA3RBFDR3I3UKNFHTJPQ5F6KOOKSGYIAM4TRQN54W-1')
) as temp (token_id, blockchain, symbol, contract_address) 