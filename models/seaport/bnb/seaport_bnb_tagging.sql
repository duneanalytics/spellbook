{{ config(
    alias = alias('tagging'),
    tags = ['dunesql', 'static'],
    unique_key = ['blockchain', 'tagging_method', 'identifier'])
}}

SELECT blockchain, tagging_method, identifier, protocol, protocol_type
FROM
(VALUES
    ('bnb', 'zone', 0x004c00500000ad104d7dbd00e3ae0a5c00560c00, 'OpenSea', 'Marketplace') -- Pausable Zone
    , ('bnb', 'zone', 0x000000e7ec00e7b300774b00001314b8610022b8, 'OpenSea', 'Marketplace')
    , ('bnb', 'zone', 0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd, 'OpenSea', 'Marketplace')
    , ('bnb', 'zone', 0xf397619df7bfd4d1657ea9bdd9df7ff888731a11, 'OpenSea', 'Marketplace')
    , ('bnb', 'tx_data_salt', 0x360c6ebe, 'OpenSea', 'Marketplace')
    ) 
    x (blockchain, tagging_method, identifier, protocol, protocol_type)