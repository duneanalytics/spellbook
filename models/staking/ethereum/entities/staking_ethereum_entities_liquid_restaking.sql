{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_liquid_restaking',
    tags = [ 'static'],
    unique_key = ['depositor_address'])
}}

SELECT depositor_address, entity, entity_unique_name, category
FROM
(VALUES
    (0xb25fe78faaefadb7249b4940ee485856df150bbe, 'Kelp DAO', 'Kelp DAO 1', 'Liquid Restaking')
    , (0x093f6c270ac22ec240f0c6fd7414ea774ca8d3e5, 'Renzo', 'Renzo 1', 'Liquid Restaking')
    , (0x2641c2ded63a0c640629f5edf1189e0f53c06561, 'Renzo', 'Renzo 2', 'Liquid Restaking')
    , (0xe0c8df4270f4342132ec333f6048cb703e7a9c77, 'Swell', 'Swell 1', 'Liquid Restaking')
    , (0xb3d9cf8e163bbc840195a97e81f8a34e295b8f39, 'Swell', 'Swell 2', 'Liquid Restaking')
    , (0x5e6342d8090665be14eeb8154c8a87b7249a4889, 'Swell', 'Swell 3', 'Liquid Restaking')
    , (0x25e821b7197b146f7713c3b89b6a4d83516b912d, 'ether.fi', 'ether.fi', 'Liquid Restaking')
    ) 
    x (depositor_address, entity, entity_unique_name, category)