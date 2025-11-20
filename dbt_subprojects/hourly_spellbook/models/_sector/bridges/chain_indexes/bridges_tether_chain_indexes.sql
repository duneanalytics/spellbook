{{ config(
    schema = 'bridges',
    alias = 'tether_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://docs.usdt0.to/technical-documentation/developer
SELECT id, blockchain, usdt0_address
    FROM (VALUES
    (30101, 'ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7)
    , (30111, 'optimism', 0x01bff41798a0bcf287b996046ca68b395dbc1071)
    , (30110, 'arbitrum', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9)
    , (30339, 'ink', 0x0200c29006150606b650577bbe7b6248f58470c1)
    , (30362, 'berachain', 0x779ded0c9e1022225f8e0630b35a9b54be713736)
    , (30109, 'polygon', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f)
    , (30383, 'plasma', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb)
    , (30320, 'unichain', 0x9151434b16b9763660705744891fa906f660ecc5)
    , (30331, 'corn', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb)
    , (30274, 'xlayer', 0x779ded0c9e1022225f8e0630b35a9b54be713736)
    , (30333, 'rootstock', 0x779ded0c9e1022225f8e0630b35a9b54be713736)
    , (30367, 'hyperevm', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb)
    , (30280, 'sei', 0x9151434b16b9763660705744891fa906f660ecc5)
    , (30295, 'flare', 0xe7cd86e13ac4309349f30b3435a9d337750fc82d)
    ) AS x (id, blockchain, usdt0_address)