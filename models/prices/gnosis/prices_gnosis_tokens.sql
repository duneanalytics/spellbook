{{ config(
        schema='prices_gnosis',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT 
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    -- Query for Popular Traded Tokens on Gnosis Chain without prices: https://dune.com/queries/1719783
    ('dai-dai', 'gnosis', 'WXDAI', 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 18),
    ('usdc-usd-coin', 'gnosis', 'USDC', 0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 6),
    ('usdt-tether', 'gnosis', 'USDT', 0x4ecaba5870353805a9f068101a40e0f32ed605c6, 6),
    ('wbtc-wrapped-bitcoin', 'gnosis', 'WBTC', 0x8e5bbbb09ed1ebde8674cda39a0c169401db4252, 8),
    ('weth-weth', 'gnosis', 'WETH', 0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1, 18),
    ('gno-gnosis', 'gnosis', 'GNO', 0x9c58bacc331c9aa871afd802db6379a98e80cedb, 18),
    ('cow-cow-protocol-token', 'gnosis', 'COW', 0x177127622c4a00f3d409b75571e12cb3c8973d3c, 18),
    ('matic-polygon', 'gnosis', 'MATIC', 0x7122d7661c4564b7c6cd4878b06766489a6028a2, 18),
    ('1inch-1inch', 'gnosis', '1INCH', 0x7f7440c5098462f833e123b44b8a03e1d9785bab, 18),
    ('dai-dai', 'gnosis', 'DAI', 0x44fa8e6f47987339850636f88629646662444217, 18),
    ('busd-binance-usd', 'gnosis', 'BUSD', 0xdd96b45877d0e8361a4ddb732da741e97f3191ff, 18),
    ('crv-curve-dao-token', 'gnosis', 'CRV', 0x712b3d230f3c1c19db860d80619288b1f0bdd0bd, 18),
    ('link-chainlink', 'gnosis', 'LINK', 0xe2e73a1c69ecf83f464efce6a5be353a37ca09b2, 18),
    ('sushi-sushi', 'gnosis', 'SUSHI', 0x2995d1317dcd4f0ab89f4ae60f3f020a4f17c7ce, 18),
    ('wsteth-wrapped-liquid-staked-ether-20', 'gnosis', 'WSTETH', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6, 18)     
) as temp (token_id, blockchain, symbol, contract_address, decimals)
