{{ config(
        schema='prices_fantom',
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

    ('weth-weth', 'fantom', 'WETH', 0x74b23882a30290451A17c44f4F05243b6b58C76d, 18),
    ('wbtc-wrapped-bitcoin', 'fantom', 'WBTC', 0x321162Cd933E2Be498Cd2267a90534A804051b11, 8),
    ('ftm-fantom', 'fantom', 'WFTM', 0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83, 18),
    ('aave-new', 'fantom', 'AAVE', 0x6a07A792ab2965C72a5B8088d3a069A7aC3a993B, 18),
    ('avax-avalanche', 'fantom', 'AVAX', 0x511D35c52a3C244E7b8bd92c0C297755FbD89212, 18),
    ('axlusdt-axelar-usd-tether', 'fantom', 'axlUSDT', 0xd226392c23fb3476274ed6759d4a478db3197d82, 6),
    ('axlatom-axelar-wrapped-atom', 'fantom', 'axlATOM', 0x3bb68cb55fc9c22511467c18e42d14e8c959c4da, 6),
    ('crv-curve-dao-token', 'fantom', 'CRV', 0x1E4F97b9f9F913c46F1632781732927B9019C68b, 18),
    ('dai-dai', 'fantom', 'DAI', 0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E, 18),
    ('frax-frax', 'fantom', 'FRAX', 0xdc301622e621166BD8E82f2cA0A26c13Ad0BE355, 18),
    ('renbtc-renbtc', 'fantom', 'renBTC', 0xDBf31dF14B66535aF65AaC99C32e9eA844e14501, 8),
    ('usdc-usd-coin', 'fantom', 'USDC', 0x04068DA6C83AFCFA0e13ba15A6696662335D5B75, 6),
    ('tusd-trueusd', 'fantom', 'TUSD', 0x9879aBDea01a879644185341F7aF7d8343556B7a, 18),
    ('fxs-frax-share', 'fantom', 'FXS', 0x7d016eec9c25232b01F23EF992D98ca97fc2AF5a, 18),
    --('mcrt-magiccraft', 'fantom', 'MCRT', 0xE705aF5f63fcaBDCDF5016aA838EAaac35D12890, 9),
    ('tarot-tarot', 'fantom', 'TAROT', 0xC5e2B037D30a390e62180970B3aa4E91868764cD, 18),
    ('brush-brush', 'fantom', 'BRUSH', 0x85dec8c4B2680793661bCA91a8F129607571863d, 18),
    -- ('tcs-timechain-swap-token', 'fantom', 'TCS', 0xFbfAE0DD49882e503982f8eb4b8B1e464ecA0b91, 18), -- inactive so commented out
    ('wbond-war-bond', 'fantom', 'WBOND', 0x59c6606ED2AF925F270733e378D6aF7829B5b3cf, 18),
    ('mimatic-mimatic', 'fantom', 'miMATIC', 0xfB98B335551a418cD0737375a2ea0ded62Ea213b, 18),
    ('fame-fantom-maker', 'fantom', 'FAME', 0x904f51a2E7eEaf76aaF0418cbAF0B71149686f4A, 18),
    ('boo-spookyswap', 'fantom', 'BOO', 0x841FAD6EAe12c286d1Fd18d1d525DFfA75C7EFFE, 18),
    ('hec-hector-dao', 'fantom', 'HEC', 0x5C4FDfc5233f935f20D2aDbA572F770c2E377Ab0, 9),
    ('geist-geist-finance', 'fantom', 'GEIST', 0xd8321AA83Fb0a4ECd6348D4577431310A6E0814d, 18),
    ('treeb-treeb', 'fantom', 'TREEB', 0xc60D7067dfBc6f2caf30523a064f416A5Af52963, 18),
    ('bsgg-betswapgg', 'fantom', 'BSGG', 0x5A33869045db8A6a16c9f351293501CFD92cf7ed, 18),
    ('sex-solidex', 'fantom', 'SEX', 0xD31Fcd1f7Ba190dBc75354046F6024A9b86014d7, 18),
    ('tomb-tomb', 'fantom', 'TOMB', 0x6c021Ae822BEa943b2E66552bDe1D2696a53fbB7, 18),
    ('tor-tor-ftm-token', 'fantom', 'TOR', 0x74E23dF9110Aa9eA0b6ff2fAEE01e740CA1c642e, 18),
    ('tshare-tomb-shares', 'fantom', 'TSHARE', 0x4cdF39285D7Ca8eB3f090fDA0C069ba5F4145B37, 18),
    ('lqdr-liquiddriver', 'fantom', 'LQDR', 0x10b620b2dbAC4Faa7D7FFD71Da486f5D44cd86f9, 18),
    ('weve-vedao', 'fantom', 'WeVE', 0x911da02C1232A3c3E1418B834A311921143B04d7, 18),
    ('beets-beethoven-x', 'fantom', 'BEETS', 0xF24Bcf4d1e507740041C9cFd2DddB29585aDCe1e, 18),
    ('beftm-beefy-escrowed-fantom', 'fantom', 'beFTM', 0x7381eD41F6dE418DdE5e84B55590422a57917886, 18),
    ('bro-dexbrowser', 'fantom', 'BRO', 0x230576a0455d7Ae33c6dEfE64182C0BB68bAFAF3, 18),
    ('oath-oath', 'fantom', 'OATH', 0x21Ada0D2aC28C3A5Fa3cD2eE30882dA8812279B6, 18),
    ('scream-scream', 'fantom', 'SCREAM', 0xe0654C8e6fd4D733349ac7E09f6f23DA256bF475, 18),
    ('yoshi-yoshiexchange', 'fantom', 'YOSHI', 0x3dc57B391262e3aAe37a08D91241f9bA9d58b570, 18),
    ('sftmx-stader-sftmx', 'fantom', 'sFTMX', 0xd7028092c830b5C8FcE061Af2E593413EbbC1fc1, 18),
    ('pgk-penguin-kart', 'fantom', 'PGK', 0x188a168280589bC3E483d77aae6b4A1d26bD22dC, 18),
    ('wshec-wrapped-hec', 'fantom', 'wsHEC', 0x94CcF60f700146BeA8eF7832820800E2dFa92EdA, 18),
    ('flibero-fantom-libero-financial', 'fantom', 'FLIBERO', 0xC3f069D7439baf6D4D6E9478D9Cc77778E62D147, 18),
    ('imx-impermax', 'fantom', 'IMX', 0xeA38F1CCF77Bf43F352636241b05dd8f6F5f52B2, 18),
    ('solid-solidly', 'fantom', 'SOLID', 0x888EF71766ca594DED1F0FA3AE64eD2941740A20, 18),
    ('mst-metaland-gameverse', 'fantom', 'MST', 0x152888854378201e173490956085c711f1DeD565, 18),
    ('3omb-3omb-token', 'fantom', '3OMB', 0x14DEf7584A6c52f470Ca4F4b9671056b22f4FfDE, 18),
    ('gscarab-gscarab', 'fantom', 'GSCARAB', 0x6ab5660f0B1f174CFA84e9977c15645e4848F5D6, 18),
    ('lif3-lif3', 'fantom', 'LIF3', 0xbf60e7414EF09026733c1E7de72E7393888C64DA, 18),
    ('oxd-0xdao', 'fantom', 'OXD', 0xc165d941481e68696f43EE6E99BFB2B23E0E3114, 18),
    ('ust-terrausd', 'fantom', 'USTC', 0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9, 6),
    ('luna-terra', 'fantom', 'LUNC', 0x95dd59343a893637be1c3228060ee6afbf6f0730, 6),
    ('2shares-2share', 'fantom', '2SHARES', 0xc54A1684fD1bef1f077a336E6be4Bd9a3096a6Ca, 18),
    ('3share-3share', 'fantom', '3SHARE', 0x6437adac543583c4b31bf0323a0870430f5cc2e7, 18),
    ('ico-axelar', 'fantom', 'AXL', 0x8b1f4432f943c465a973fedc6d7aa50fc96f1f65, 6),
    ('grain-granary', 'fantom', 'GRAIN', 0x02838746d9e1413e07ee064fcbada57055417f21, 18),
    ('mim-magic-internet-money', 'fantom', 'MIM', 0x82f0b8b456c1a451378467398982d4834b6829c1, 18),
    ('link-chainlink', 'fantom', 'LINK', 0xb3654dc3d10ea7645f8319668e8f54d2574fbdc8, 18),
    ('axlusdc-axelar-wrapped-usdc', 'fantom', 'AXLUSDC', 0x1b6382dbdea11d97f24495c9a90b7c88469134a4, 6),
    ('orbs-orbs', 'fantom', 'ORBS', 0x43a8cab15d06d3a5fe5854d714c37e7e9246f170, 18),
    --('stg-stargate-finance', 'fantom', 'STG', 0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590, 18), --not found in API 
    ('spa-spartacus', 'fantom', 'SPA', 0x5602df4a94eb6c680190accfa2a475621e0ddbdc, 9),
    ('space-space-token', 'fantom', 'SPACE', 0x5f7f94a1dd7b15594d17543beb8b30b111dd464c, 18),
    ('woo-wootrade', 'fantom', 'WOO', 0x6626c47c00f1d87902fc13eecfac3ed06d5e8d8a, 18),
    ('cekke-cekke-cronje', 'fantom', 'CEKKE', 0x3bc34d8Ace32D768a3F76e17AAEF2B1D8f261e1D, 18),
    --('vemp-vemp', 'fantom', 'VEMP', 0x526f1dc408cfe7fc5330ab9f1e78474ceff2a5dd, 18) --not found in API 
    ('unidx-unidex', 'fantom', 'UNIDX', 0x0483a76D80D0aFEC6bd2afd12C1AD865b9DF1471, 18),
    ('bay-moon-bay', 'fantom', 'BAY', 0xd361474bB19C8b98870bb67F5759cDF277Dee7F9, 18),
    ('usdc-usdc-stargate-bridge', 'fantom', 'USDC', 0x28a92dde19D9989F39A49905d7C9C2FAc7799bDf, 6),
    ('fusdt-frapped-usdt', 'fantom', 'FUSDT', 0x049d68029688eabf473097a2fc38ef61633a3c7a, 6)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
