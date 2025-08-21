{{ config(
    schema = 'bridges',
    alias = 'cctp_chain_indexes',
    materialized = 'view',
    )
}}

SELECT id, blockchain
    FROM (VALUES
    (0, 'ethereum')
    , (1, 'avalanche_c')
    , (2, 'optimism')
    , (3, 'arbitrum')
    , (4, 'noble')
    , (5, 'solana')
    , (6, 'base')
    , (7, 'polygon')
    , (8, 'sui')
    , (9, 'aptos')
    , (10, 'unichain')
    , (11, 'linea')
    , (12, 'codex')
    , (13, 'sonic')
    , (14, 'worldchain')
    , (16, 'sei')
    , (17, 'bnb')
    ) AS x (id, blockchain)