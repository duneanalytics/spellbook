{% set blockchain = 'robinhood' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth', 'WETH', 0x0bd7D308F8E1639FaB988DF18A8011f41eACad73, 18)
    , ('usdg-global-dollar', 'USDG', 0x5fc5360D0400a0Fd4F2Af552ADd042d716f1D168, 6)
    , ('aapl-apple-robinhood-tokenized-stock', 'AAPL', 0xaf3d76f1834a1d425780943c99ea8a608f8a93f9, 18)
    , ('ai-artificial-inu-2', 'AI', 0x2e8c31162b855a2ffa90f6f8634643ad6f111e18, 18)
    , ('amd-amd-robinhood-tokenized-stock', 'AMD', 0x86923f96303d656e4aa86d9d42d1e57ad2023fdc, 18)
    , ('amzn-amazon-robinhood-tokenized-stock', 'AMZN', 0x12f190a9f9d7d37a250758b26824b97ce941bf54, 18)
    , ('au-autsm', 'AU', 0xd4aae326ddd1a2537a92c00e6576e4605e5d1e18, 18)
    , ('boner-boner-coin', 'BONER', 0x98096d17e191b3da1d5f99a6d7b3584351b11e18, 18)
    , ('cashcat-cash-cat', 'CASHCAT', 0x020bfc650a365f8bb26819deaabf3e21291018b4, 18)
    , ('gme-gamestop-robinhood-tokenized-stock', 'GME', 0x1b0e319c6a659f002271b69db8a7df2f911c153e, 18)
    , ('openaix1l-openai-1x-long', 'OPENAIx1L', 0xfe09fb328be1c286b4f597ed34764b7472ae72c5, 18)
    , ('pack-wolf-pack', 'PACK', 0x0145acbccefbed6f303c420beeaaac72e905430b, 18)
    , ('pons-pons', 'PONS', 0x39dbed3a2bd333467115de45665cc57f813c4571, 18)
    , ('rddt-reddit-robinhood-tokenized-stock', 'RDDT', 0x05b37fb53a299a1b874a619e1c4c404d52c36f4c, 18)
    , ('rvh-ravenhood', 'RVH', 0x96765066f6a040a21eb027167d2315b707c82633, 18)
    , ('schiffy-schiffy', 'SCHIFFY', 0x42afa2124ca5a2b83898e46b2da9a190995b1e18, 18)
    , ('shroom-mushroom', 'SHROOM', 0xab093def657f15df31b33922a95e047add645b29, 18)
    , ('slv-ishares-silver-trust-robinhood-tokenized-stock', 'SLV', 0x411efb0e7f985935daec3d4c3ebaea0d0ad7d89f, 18)
    , ('spacex-spacex-robinhood-tokenized-stock', 'SPCX', 0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea, 18)
    , ('spy-spdr-sampp-500-etf-trust-robinhood-tokenized-stock', 'SPY', 0x117cc2133c37b721f49de2a7a74833232b3b4c0c, 18)
    , ('stonkbroker-stonkbroker', 'STONKBROKER', 0xe934e36a439c94017b64a3fece66af12099abf50, 18)
    , ('tsla-tesla-robinhood-tokenized-stock', 'TSLA', 0x322f0929c4625ed5bad873c95208d54e1c003b2d, 18)
    , ('vex-projectvex', 'VEX', 0x8ff92566f2e81bdd68edfaa8cde73942a723796b, 18)
    , ('xrp-xrp-robinhood', 'XRP', 0x6c866e8396a7e9931f5338f89e689f4699b66b03, 18)
) as temp (token_id, symbol, contract_address, decimals)
