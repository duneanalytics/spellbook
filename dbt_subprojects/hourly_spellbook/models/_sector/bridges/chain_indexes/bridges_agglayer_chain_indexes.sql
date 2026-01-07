{{ config(
    schema = 'bridges',
    alias = 'agglayer_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://build.agglayer.dev/chains

SELECT id, blockchain
    FROM (VALUES
    (0, 'ethereum')
    , (20, 'katana')
    , (16, 'pentagon_games')
    , (10, 'silicon_network')
    , (3, 'xlayer')
    , (8, 'wirex_pay')
    , (13, 'ternoa')
    , (22, 'forknet')
    , (37, 'bokuto')
    , (35, 'lumia')
    ) AS x (id, blockchain)