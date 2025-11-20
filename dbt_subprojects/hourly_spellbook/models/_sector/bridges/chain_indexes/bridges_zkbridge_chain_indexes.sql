{{ config(
    schema = 'bridges',
    alias = 'zkbridge_chain_indexes',
    materialized = 'view',
    )
}}

SELECT id, blockchain
    FROM (VALUES
    (2, 'ethereum')
    , (3, 'bnb')
    , (4, 'polygon')
    , (7, 'optimism')
    , (8, 'arbitrum')
    , (19, 'linea')
    , (20, 'mantle')
    , (22, 'base')
    , (23, 'opbnb')
    , (26, 'scroll')
    ) AS x (id, blockchain)