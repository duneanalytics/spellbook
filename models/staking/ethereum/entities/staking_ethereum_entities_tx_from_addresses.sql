{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_tx_from_addresses',
    tags = [ 'static'],
    unique_key = ['tx_from'])
}}

SELECT tx_from, entity, entity_unique_name, category
FROM
(VALUES
    (0x4f80ce44afab1e5e940574f135802e12ad2a5ef0, 'Octant', 'Octant', 'Staking Pool')
    --, (0xA0766B65A4f7B1da79a1AF79aC695456eFa28644, 'Manifold Finance', 'Manifold Finance 1', 'Staking Pool')
    ) 
    x (tx_from, entity, entity_unique_name, category)