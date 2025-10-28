{{ config(
    schema = 'bridges',
    alias = 'butter_chain_indexes',
    materialized = 'view',
    )
}}

SELECT id, blockchain
    FROM (VALUES
    (1, 'ethereum')
    , (10, 'optimism')
    , (56, 'bnb')
    , (127, 'polygon')
    , (137, 'polygon')
    , (324, 'zksync')
    , (1030, 'conflux')
    , (5000, 'mantle')
    , (8217, 'kaia')
    , (8453, 'base')
    , (22776, 'map')
    , (42161, 'arbitrum')
    , (59144, 'linea')
    , (81457, 'blast')
    , (534352, 'scroll')
    ) AS x (id, blockchain)