{{ config(
    schema = 'bridges',
    alias = 'allbridge_classic_chain_indexes',
    materialized = 'view',
    )
}}
]

SELECT blockchain, allbridge_slug
    FROM (VALUES
    ('solana', 'SOL')
    , ('bnb', 'BSC')
    , ('terra', 'TRA')
    , ('celo', 'CELO')
    , ('aurora', 'AURO')
    , ('starknet', 'STKZ')
    , ('fantom', 'FTM')
    , ('polygon', 'POL')
    , ('ethereum', 'ETH')
    , ('near', 'NEAR')
    , ('avalanche_c', 'AVA')
    , ('tezos', 'TEZ')
    , ('heco', 'HECO')
    , ('waves', 'WAVE')
    , ('fuse', 'FUSE')
    , ('harmony', 'HRM')
    , ('ripple', 'XRPL')
    , ('koii', 'KOII')
    , ('stacks', 'STKS')
    , ('klaytn', 'KLAY')
    , ('casper', 'CSPR')
    --, ('', 'ZZZ')
    , ('solana', 'SOL0')
    , ('stellar', 'XLM')
    ) AS x (blockchain, allbridge_slug)