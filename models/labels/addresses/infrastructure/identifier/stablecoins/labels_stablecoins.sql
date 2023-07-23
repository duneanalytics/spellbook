{{config(tags=['dunesql'],
    alias = alias('stablecoins'),
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "solana", "arbitrum", "optimism", "fantom", "avalanche_c", "gnosis"]\',
                                "sector",
                                "labels",
                                \'["hildobby"]\') }}'
)}}

SELECT blockchain, address as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(   
    -- Sources:
    -- https://stablecoins.wtf/
    -- https://www.coingecko.com/en/categories/stablecoins

    VALUES
    -- Tether USD
    ('ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x55d398326f99059ff775485246999027b3197955, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
--     , ('solana', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x049d68029688eabf473097a2fc38ef61633a3c7a, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('gnosis', 0x4ecaba5870353805a9f068101a40e0f32ed605c6, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- MarkerDAO DAI   
    , ('ethereum', 0x6b175474e89094c44da98b954eedeac495271d0f, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('gnosis', 0x44fa8e6f47987339850636f88629646662444217, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- USD Coin
    , ('ethereum', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x2791bca1f2de4661ed88a30c99a7a9449aa84174, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x7f5c764cbc14f9669b88837ca1490cca17c31607, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x04068da6c83afcfa0e13ba15a6696662335d5b75, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('gnosis', 0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Binance USD
    , ('ethereum', 0x4fabb145d64652a948d72533023f6e7a623c7c53, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xe9e7cea3dedca5984780bafc599bd69add087d56, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('gnosis', 0xdd96b45877d0e8361a4ddb732da741e97f3191ff, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- TrueUSD
    , ('ethereum', 0x0000000000085d4780b73119b644ae5ecd22b376, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x14016e85a25aeb13065688cafb43044c2ef86784, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x2e1ad108ff1d8c782fcbbb89aad783ac49586756, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x9879abdea01a879644185341f7af7d8343556b7a, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x4d15a3a2286d883af0aa1b3f21367843fac63e07, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x1c20e891bab6b1727d14da358fae2984ed9b59eb, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- FRAX
    , ('ethereum', 0x853d955acef822db058eb8505911ed77f175b99e, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x90c97f71e18723b0cf0dfa30ee176ab653e89f40, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x45c32fa6df82ead1e2ef74d17b76547eddfaff89, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0xdc301622e621166bd8e82f2ca0a26c13ad0be355, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x17fc002b466eec40dae837fc4be5c67993ddbd6f, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x2e3d870790dc77a83dd1d18184acc7439a53f475, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Pax Dollar
    , ('ethereum', 0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- USDD
    , ('ethereum', 0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xd17479997f34dd9156deef8f95a52d81d265be9c, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x680447595e8b7b3aa1b43beb9f6098c79ac2ab3f, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0xb514cabd09ef5b169ed3fe0fa8dbd590741e81c2, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
     -- Gemini Dollar
    , ('ethereum', 0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Liquity USD
    , ('ethereum', 0x5f98805a4e8be255a32880fdec7f6728c6568ba0, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x23001f892c0c82b79303edc9b9033cd190bb21c7, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xc40f949f8a4e094d1b49a23ea9241d289b7b2819, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Magic Internet Money
    , ('ethereum', 0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xfe19f0b51438fd612f6fd59c1dbb3ea319f433ba, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x49a0400587a7f65072c87c4910449fdcc5c47242, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x82f0b8b456c1a451378467398982d4834b6829c1, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x130966628846bfd36ff31a822705796e8cb8c18d, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Origin Dollar
    , ('ethereum', 0x2a8e1e676ec238d8a992307b495b45b3feaa5e86, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Dola
    , ('ethereum', 0x865377367054516e17014ccded1e7d814edc9ce4, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x2f29bc0ffaf9bff337b31cbe6cb5fb3bf12e5840, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x3129662808bec728a27ab6a6b9afd3cbaca8a43c, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x8ae125e8653821e851f12a49f7765db9a9ce7384, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- flexUSD
    , ('ethereum', 0xa774ffb4af6b0a91331c084e1aebae6ad535e6f3, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- sUSD
    , ('ethereum', 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x0e1694483ebb3b74d3054e383840c6cf011e518e, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xa970af1a584579b618be4d69ad6f73459d112f95, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- MAI MIMATIC
    , ('ethereum', 0x8d6cebd76f18e1558d4db88138e2defb3909fad6, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0xa3fa99a148fa48d14ed51d610c367c61876997f1, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
--     , ('solana', '9mWRABuz2x6koTPCWiCPM49WUbcrNqGTHBV9T9k7y1o7', 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0xfb98b335551a418cd0737375a2ea0ded62ea213b, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xdfa46478f9e5ea86d57387849598dbfb2e964b02, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('gnosis', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Fei USD
    , ('ethereum', 0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Bean (old & new contract)
    , ('ethereum', 0xdc59ac4fefa32293a95889dc396682858d52e5db, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('ethereum', 0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Reserve
    , ('ethereum', 0x196f4727526ea7fb1e17b2071b3d8eaa38486988, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- HUSD
    , ('ethereum', 0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Alchemix USD
    , ('ethereum', 0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0xb67fa6defce4042070eb1ae1511dcd6dcc6a532e, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- BOB
    , ('ethereum', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- TOR
    , ('bnb', 0x1d6cbdc6b29c6afbae65444a1f65ba9252b8ca83, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0x74e23df9110aa9ea0b6ff2faee01e740ca1c642e, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- YUSD Stablecoin
    , ('avalanche_c', 0x111111111111ed1d73f860f57b2798b683f2d325, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Vesta Stable
    , ('arbitrum', 0x64343594ab9b56e99087bfa6f2335db24c2d1f17, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- USDK
    , ('ethereum', 0x1c48f86ae57291f7686349f12601910bd8d470bb, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- mStable USD
    , ('ethereum', 0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Vai
    , ('bnb', 0x4bd17003473389a42daf6a0a729f6fdb328bbbd7, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Coin98 Dollar
    , ('ethereum', 0xc285b7e09a4584d027e5bc36571785b515898246, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xfa4ba88cf97e282c505bea095297786c16070129, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
--     , ('solana', 'CUSDvqAQLbt7fRofcmV2EXfPA2t36kzj7FjzdmqDiNQL', 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
     -- USDP Stablecoin
    , ('ethereum', 0x1456688345527be1f37e9e627da0837d6f08c925, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Sperax USD
    , ('arbitrum', 0xd74f5255d557944cf7dd0e45ff521520002d5748, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- ZUSD
    , ('ethereum', 0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa, 'Fiat-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- SpiceUSD
    , ('ethereum', 0x45fdb1b92a649fb6a64ef1511d3ba5bf60044838, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xde7d1ce109236b12809c45b23d22f30dba0ef424, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x2f1b1662a895c6ba01a99dcaf56778e7d77e5609, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0xab05b04743e0aeaf9d2ca81e5d3b8385e4bf961e, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- BondAppetite USD
    , ('ethereum', 0x9a1997c130f4b2997166975d9aff92797d5134c2, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Zasset zUSD
    , ('bnb', 0xf0186490b18cb74619816cfc7feb51cdbe4ae7b9, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- USD Balance
    , ('fantom', 0x6fc9383486c163fa48becdec79d6058f984f62ca, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Inflation Adjusted USDS
    , ('ethereum', 0xf9c2b386ff5df088ac717ab0010587bad3bc1ab1, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x6458df5d764284346c19d88a104fd3d692471499, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x66f31345cb9477b427a1036d43f923a557c432a4, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('avalanche_c', 0x8861f5c40a0961579689fdf6cdea2be494f9b25a, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- xDollar Interverse Money
    , ('ethereum', 0x573d2505a7ee69d136a8667b4cd915f039ac54e5, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- xDollar Stablecoin
    , ('polygon', 0x3a3e7650f8b9f667da98f236010fbf44ee4b2975, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x3509f19581afedeff07c53592bc0ca84e4855475, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Wrapped USDR
    , ('ethereum', 0xd5a14081a34d256711b02bbef17e567da48e80b5, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x2952beb1326accbb5243725bd4da2fc937bca087, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0xaf0d9d65fc54de245cda37af3d18cbec860a4d4b, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0xddc0385169797937066bbd8ef409b5b3c0dfeb52, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0x340fe1d898eccaad394e2ba0fc1f93d27c7b717a, 'RWA-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Wrapped USTC
    , ('ethereum', 0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0x23396cf899ca06c4472205fc903bdb4de249d6fc, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0x692597b009d13c4049a947cab2239b7d6517875f, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
--     , ('solana', 'CXLBjMMcwkc17GfJtBos6rQCo1ypeH6eDbB82Kby4MRm', 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('fantom', 0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9, 'Algorithmic stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Synth oUSD
    , ('bnb', 0x6bf2be9468314281cd28a94c35f967cafd388325, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- dForce USD
    , ('ethereum', 0x0a5e677a6a24b2f1a2bf4f3bffc443231d2fdec8, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('bnb', 0xb5102cee1528ce2c760893034a4603663495fd72, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('polygon', 0xcf66eb3d546f0415b368d98a95eaf56ded7aa752, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('arbitrum', 0x641441c631e2f909700d2f41fd87f0aa6a6b4edb, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    , ('optimism', 0xbfd291da8a403daaf7e5e9dc1ec0aceacd4848b9, 'Crypto-backed stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Coffin Dollar
    , ('fantom', 0x0def844ed26409c5c46dda124ec28fb064d90d27, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    -- Iron
    , ('polygon', 0xd86b5923f3ad7b585ed81b448170ae026c65ae9a, 'Hybrid stablecoin', 'infrastructure', 'hildobby', 'static', TIMESTAMP '2023-03-02' , now(), 'stablecoins', 'identifier')
    ) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
