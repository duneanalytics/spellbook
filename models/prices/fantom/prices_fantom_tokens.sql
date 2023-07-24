{{ config(
        schema='prices_fantom',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags=['static', 'dunesql']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    ('weth-weth', 'fantom', 'WETH', 0x74b23882a30290451a17c44f4f05243b6b58c76d, 18), 
    ('wbtc-wrapped-bitcoin', 'fantom', 'WBTC', 0x321162cd933e2be498cd2267a90534a804051b11, 8),
    ('ftm-fantom', 'fantom', 'WFTM', 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83, 18),
    ('aave-new', 'fantom', 'AAVE', 0x6a07a792ab2965c72a5b8088d3a069a7ac3a993b, 18), 
    ('avax-avalanche', 'fantom', 'AVAX', 0x511d35c52a3c244e7b8bd92c0c297755fbd89212, 18), 
    ('axlusdc-axelar-usd-coin', 'fantom', 'axlUSDC', 0x1b6382dbdea11d97f24495c9a90b7c88469134a4, 6),
    ('axlusdt-axelar-usd-tether', 'fantom', 'axlUSDT', 0xd226392c23fb3476274ed6759d4a478db3197d82, 6),
    ('axlatom-axelar-wrapped-atom', 'fantom', 'axlATOM', 0x3bb68cb55fc9c22511467c18e42d14e8c959c4da, 6),
    ('crv-curve-dao-token', 'fantom', 'CRV', 0x1e4f97b9f9f913c46f1632781732927b9019c68b, 18),
    ('dai-dai', 'fantom', 'DAI', 0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e, 18),
    ('frax-frax', 'fantom', 'FRAX', 0xdc301622e621166bd8e82f2ca0a26c13ad0be355, 18),
    ('renbtc-renbtc', 'fantom', 'renBTC', 0xdbf31df14b66535af65aac99c32e9ea844e14501, 8),
    ('usdc-usd-coin', 'fantom', 'USDC', 0x04068da6c83afcfa0e13ba15a6696662335d5b75, 6), 
    ('tusd-trueusd', 'fantom', 'TUSD', 0x9879abdea01a879644185341f7af7d8343556b7a, 18), 
    ('fxs-frax-share', 'fantom', 'FXS', 0x7d016eec9c25232b01f23ef992d98ca97fc2af5a, 18),
    --('mcrt-magiccraft', 'fantom', 'MCRT', 0xe705af5f63fcabdcdf5016aa838eaaac35d12890, 9), 
    ('tarot-tarot', 'fantom', 'TAROT', 0xc5e2b037d30a390e62180970b3aa4e91868764cd, 18),
    ('brush-brush', 'fantom', 'BRUSH', 0x85dec8c4b2680793661bca91a8f129607571863d, 18), 
    -- ('tcs-timechain-swap-token', 'fantom', 'TCS', 0xfbfae0dd49882e503982f8eb4b8b1e464eca0b91, 18), -- inactive so commented out
    ('wbond-war-bond', 'fantom', 'WBOND', 0x59c6606ed2af925f270733e378d6af7829b5b3cf, 18), 
    ('mimatic-mimatic', 'fantom', 'miMATIC', 0xfb98b335551a418cd0737375a2ea0ded62ea213b, 18),
    ('fame-fantom-maker', 'fantom', 'FAME', 0x904f51a2e7eeaf76aaf0418cbaf0b71149686f4a, 18), 
    ('boo-spookyswap', 'fantom', 'BOO', 0x841fad6eae12c286d1fd18d1d525dffa75c7effe, 18), 
    ('hec-hector-dao', 'fantom', 'HEC', 0x5c4fdfc5233f935f20d2adba572f770c2e377ab0, 9),
    ('geist-geist-finance', 'fantom', 'GEIST', 0xd8321aa83fb0a4ecd6348d4577431310a6e0814d, 18),
    ('treeb-treeb', 'fantom', 'TREEB', 0xc60d7067dfbc6f2caf30523a064f416a5af52963, 18), 
    ('bsgg-betswapgg', 'fantom', 'BSGG', 0x5a33869045db8a6a16c9f351293501cfd92cf7ed, 18), 
    ('sex-solidex', 'fantom', 'SEX', 0xd31fcd1f7ba190dbc75354046f6024a9b86014d7, 18),
    ('tomb-tomb', 'fantom', 'TOMB', 0x6c021ae822bea943b2e66552bde1d2696a53fbb7, 18), 
    ('tor-tor-ftm-token', 'fantom', 'TOR', 0x74e23df9110aa9ea0b6ff2faee01e740ca1c642e, 18),
    ('tshare-tomb-shares', 'fantom', 'TSHARE', 0x4cdf39285d7ca8eb3f090fda0c069ba5f4145b37, 18),
    ('lqdr-liquiddriver', 'fantom', 'LQDR', 0x10b620b2dbac4faa7d7ffd71da486f5d44cd86f9, 18), 
    ('weve-vedao', 'fantom', 'WeVE', 0x911da02c1232a3c3e1418b834a311921143b04d7, 18), 
    ('beets-beethoven-x', 'fantom', 'BEETS', 0xf24bcf4d1e507740041c9cfd2dddb29585adce1e, 18), 
    ('beftm-beefy-escrowed-fantom', 'fantom', 'beFTM', 0x7381ed41f6de418dde5e84b55590422a57917886, 18), 
    ('bro-dexbrowser', 'fantom', 'BRO', 0x230576a0455d7ae33c6defe64182c0bb68bafaf3, 18),
    ('oath-oath', 'fantom', 'OATH', 0x21ada0d2ac28c3a5fa3cd2ee30882da8812279b6, 18),
    ('scream-scream', 'fantom', 'SCREAM', 0xe0654c8e6fd4d733349ac7e09f6f23da256bf475, 18),
    ('yoshi-yoshiexchange', 'fantom', 'YOSHI', 0x3dc57b391262e3aae37a08d91241f9ba9d58b570, 18), 
    ('sftmx-stader-sftmx', 'fantom', 'sFTMX', 0xd7028092c830b5c8fce061af2e593413ebbc1fc1, 18), 
    ('pgk-penguin-kart', 'fantom', 'PGK', 0x188a168280589bc3e483d77aae6b4a1d26bd22dc, 18), 
    ('wshec-wrapped-hec', 'fantom', 'wsHEC', 0x94ccf60f700146bea8ef7832820800e2dfa92eda, 18), 
    ('flibero-fantom-libero-financial', 'fantom', 'FLIBERO', 0xc3f069d7439baf6d4d6e9478d9cc77778e62d147, 18), 
    ('imx-impermax', 'fantom', 'IMX', 0xea38f1ccf77bf43f352636241b05dd8f6f5f52b2, 18), 
    ('solid-solidly', 'fantom', 'SOLID', 0x888ef71766ca594ded1f0fa3ae64ed2941740a20, 18), 
    ('mst-metaland-gameverse', 'fantom', 'MST', 0x152888854378201e173490956085c711f1ded565, 18),
    ('3omb-3omb-token', 'fantom', '3OMB', 0x14def7584a6c52f470ca4f4b9671056b22f4ffde, 18), 
    ('gscarab-gscarab', 'fantom', 'GSCARAB', 0x6ab5660f0b1f174cfa84e9977c15645e4848f5d6, 18), 
    ('lif3-lif3', 'fantom', 'LIF3', 0xbf60e7414ef09026733c1e7de72e7393888c64da, 18),
    ('oxd-0xdao', 'fantom', 'OXD', 0xc165d941481e68696f43ee6e99bfb2b23e0e3114, 18),
    ('ust-terrausd', 'fantom', 'USTC', 0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9, 6),
    ('luna-terra', 'fantom', 'LUNC', 0x95dd59343a893637be1c3228060ee6afbf6f0730, 6),
    ('2shares-2share', 'fantom', '2SHARES', 0xc54a1684fd1bef1f077a336e6be4bd9a3096a6ca, 18), 
    ('3share-3share', 'fantom', '3SHARE', 0x6437adac543583c4b31bf0323a0870430f5cc2e7, 18),
    ('ico-axelar', 'fantom', 'AXL', 0x8b1f4432f943c465a973fedc6d7aa50fc96f1f65, 6),   
    ('grain-granary', 'fantom', 'GRAIN', 0x02838746d9e1413e07ee064fcbada57055417f21, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
