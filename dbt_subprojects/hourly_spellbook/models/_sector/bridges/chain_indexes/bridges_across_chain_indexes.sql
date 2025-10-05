{{ config(
    schema = 'bridges',
    alias = 'across_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://docs.across.to/reference/supported-chains

SELECT id, blockchain
    FROM (VALUES
    (1, 'ethereum')
    , (10, 'optimism')
    , (137, 'polygon')
    , (42161, 'arbitrum')
    , (56, 'bnb')
    , (324, 'zksync')
    , (59144, 'linea')
    , (8453, 'base')
    , (7777777, 'zora')
    , (81457, 'blast')
    , (34443, 'mode')
    , (232, 'lens')
    , (57073, 'ink')
    , (1135, 'lisk')
    , (41455, 'aleph_zero')
    , (690, 'redstone')
    , (534352, 'scroll')
    , (1868, 'soneium')
    , (480, 'worldchain')
    , (130, 'unichain')
    , (999, 'hyperevm')
    , (34268394551451, 'solana')
    ) AS x (id, blockchain)