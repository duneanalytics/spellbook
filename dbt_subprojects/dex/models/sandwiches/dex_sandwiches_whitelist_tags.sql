{{ config(
    schema='dex',
    alias = 'sandwiches_whitelist_tags',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    address
    , entity
    , blockchain
FROM (
    VALUES
    (0x6aba0315493b7e6989041c91181337b662fb1b90, 'Binance', 'ethereum')
    , (0x6aba0315493b7e6989041c91181337b662fb1b90, 'Binance', 'bnb')
    , (0x6aba0315493b7e6989041c91181337b662fb1b90, 'Binance', 'base')
) AS x (address, entity, blockchain)