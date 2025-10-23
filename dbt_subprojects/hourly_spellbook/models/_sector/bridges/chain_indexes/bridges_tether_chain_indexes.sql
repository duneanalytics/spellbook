{{ config(
    schema = 'bridges',
    alias = 'tether_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://docs.usdt0.to/technical-documentation/developer
SELECT id, blockchain, usdt0_address
    FROM (VALUES
    (30101, 'ethereum', 0x00)
    , (30111, 'optimism', 0x00)
    , (30110, 'arbitrum', 0x00)
    , (30339, 'ink', 0x00)
    , (30362, 'berachain', 0x00)
    , (30109, 'polygon', 0x00)
    , (30383, 'plasma', 0x00)
    , (30320, 'unichain', 0x00)
    , (30331, 'corn', 0x00)
    , (30274, 'xlayer', 0x00)
    , (30333, 'rootstock', 0x00)
    , (30367, 'hyperevm', 0x00)
    , (30280, 'sei', 0x00)
    , (30295, 'flare', 0x00)
    ) AS x (id, blockchain, usdt0_address)