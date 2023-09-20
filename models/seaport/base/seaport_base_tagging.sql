{{ config(
    alias = alias('tagging'),
    tags = ['dunesql', 'static'],
    unique_key = ['blockchain', 'tagging_method', 'identifier'])
}}

SELECT blockchain, tagging_method, identifier, protocol, protocol_type
FROM
(VALUES
    ('base', 'zone', 0x000000e7ec00e7b300774b00001314b8610022b8, 'OpenSea', 'Marketplace')
    , ('base', 'zone', 0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd, 'OpenSea', 'Marketplace')
    , ('base', 'tx_data_salt', 0x360c6ebe, 'OpenSea', 'Marketplace')
    ) 
    x (blockchain, tagging_method, identifier, protocol, protocol_type)