{{ config(
        schema='prices_polygon',
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
    ('aave-new','polygon','AAVE',0xd6df932a45c0f255f85145f286ea0b292b21c90b,18),
    ('ageur-ageur','polygon','agEUR',0xe0b52e49357fd4daf2c15e02058dce6bc0057db4,18),
    ('axlusdt-axelar-usd-tether', 'polygon', 'axlUSDT', 0xceed2671d8634e3ee65000edbbee66139b132fbf, 6),
    ('axlatom-axelar-wrapped-atom', 'polygon', 'axlATOM', 0x33f8a5029264bcfb66e39157af3fea3e2a8a5067, 6),
    ('bal-balancer','polygon','BAL',0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3,18),
    ('dpi-defi-pulse-index', 'polygon', 'DPI', 0x85955046df4668e1dd369d2de9f3aeb98dd2a369, 18),
    ('eurs-stasis-eurs','polygon','EURS',0xe111178a87a3bff0c8d18decba5798827539ae99,2),
    ('matic-polygon', 'polygon', 'MATIC', 0x0000000000000000000000000000000000001010, 18),
    ('dai-dai', 'polygon', 'DAI', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063, 18),
    ('dquick-dragon-quick', 'polygon', 'dQUICK', 0x958d208Cdf087843e9AD98d23823d32E17d723A1, 18),
    ('usdc-usd-coin', 'polygon', 'USDC.e', 0x2791bca1f2de4661ed88a30c99a7a9449aa84174, 6),
    ('usdc-usd-coin', 'polygon', 'USDC', 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359, 6),
    ('usdt-tether', 'polygon', 'USDT', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f, 6),
    ('stmatic-lido-staked-matic','polygon','stMATIC',0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4,18),
    ('sushi-sushi','polygon','SUSHI',0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a,18),
    ('wbtc-wrapped-bitcoin', 'polygon', 'WBTC', 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6, 8),
    ('bets-betswirl', 'polygon', 'BETS', 0x9246a5f10a79a5a939b0c2a75a3ad196aafdb43b, 18),
    ('eth-ethereum', 'polygon', 'WETH', 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619, 18),
    ('matic-polygon', 'polygon', 'WMATIC', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 18),
    ('link-chainlink', 'polygon', 'LINK', 0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39, 18),
    ('ghst-aavegotchi', 'polygon', 'GHST', 0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7, 18),
    ('gltr-gax-liquidity-token-reward', 'polygon', 'GLTR', 0x3801C3B3B5c98F88a9c9005966AA96aa440B9Afc, 18),
    ('fud-fud', 'polygon', 'FUD', 0x403e967b044d4be25170310157cb1a4bf10bdd0f, 18),
    ('fomo-fomo', 'polygon', 'FOMO', 0x44A6e0BE76e1D9620A7F76588e4509fE4fa8E8C8, 18),
    ('alpha-alpha', 'polygon', 'ALPHA', 0x6a3e7c3c6ef65ee26975b12293ca1aad7e1daed2, 18),
    ('crv-curve-dao-token', 'polygon', 'CRV', 0x172370d5cd63279efa6d502dab29171933a610af, 18),
    ('mimatic-mimatic', 'polygon', 'MIMATIC', 0xa3fa99a148fa48d14ed51d610c367c61876997f1, 18),
    ('kom-kommunitas', 'polygon', 'KOM', 0xc004e2318722ea2b15499d6375905d75ee5390b8, 8),
    ('bob-bob', 'polygon', 'BOB', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 18),
    ('ric-ricochet','polygon','RIC',0x263026e7e53dbfdce5ae55ade22493f828922965,18),
    ('nuls-nuls', 'polygon', 'NULS', 0x8b8e48a8cc52389cd16a162e5d8bd514fabf4ba0, 8),
    ('blank-blockwallet','polygon', 'BLANK', 0xf4C83080E80AE530d6f8180572cBbf1Ac9D5d435, 18),
    ('fin-fin', 'polygon', 'FIN', 0x576c990a8a3e7217122e9973b2230a3be9678e94, 18),
    ('sphere-sphere-finance', 'polygon', 'SPHERE', 0x62f594339830b90ae4c084ae7d223ffafd9658a7, 18),
    ('luna-luna-wormhole', 'polygon', 'LUNA', 0x9cd6746665d9557e1b9a775819625711d0693439, 6),
    ('ust-terrausd', 'polygon', 'USTC', 0x692597b009d13c4049a947cab2239b7d6517875f, 18),
    ('maticx-liquid-staking-matic-pos','polygon','MATICX',0xfa68fb4628dff1028cfec22b4162fccd0d45efb6,18),
    ('zed-zed-run', 'polygon', 'ZED', 0x5ec03c1f7fa7ff05ec476d19e34a22eddb48acdc, 18),
    ('polydoge-polydoge','polygon','PolyDoge',0x8a953cfe442c5e8855cc6c61b1293fa648bae472,18),
    ('gns-gains-network', 'polygon', 'GNS', 0xe5417af564e4bfda1c483642db72007871397896, 18),
    ('ico-axelar', 'polygon', 'AXL', 0x6e4e624106cb12e168e6533f8ec7c82263358940, 6),
    ('frax-frax', 'polygon', 'FRAX', 0x45c32fa6df82ead1e2ef74d17b76547eddfaff89, 18),
    ('fxs-frax-share', 'polygon', 'FXS', 0x1a3acf6D19267E2d3e7f898f42803e90C9219062, 18),
    ('sand-the-sandbox', 'polygon', 'SAND', 0xbbba073c31bf03b8acf7c28ef0738decf3695683, 18),
    ('wsteth-wrapped-liquid-staked-ether-20', 'polygon', 'WSTETH', 0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd, 18),
    ('lcd-lucidao', 'polygon', 'LCD', 0xc2a45fe7d40bcac8369371b08419ddafd3131b4a, 18),
    ('revv-revv', 'polygon', 'REVV', 0x70c006878a5a50ed185ac4c87d837633923de296, 18),
    ('grain-granary','polygon','GRAIN',0x8429d0afade80498eadb9919e41437a14d45a00b,18),
    ('oath-oath','polygon','OATH',0xc2c52ff5134596f5ff1b1204d3304228f2432836,18),
    ('tetu-tetu-reward-token','polygon','TETU',0x255707B70BF90aa112006E1b07B9AeA6De021424,18),
    ('quick-quickswap','polygon','QUICK',0x831753dd7087cac61ab5644b308642cc1c33dc13,18),
    ('dimo-dimo','polygon','DIMO',0xe261d618a959afffd53168cd07d12e37b26761db,18),
    ('gddy-giddy','polygon','GDDY',0x67eb41a14c0fe5cd701fc9d5a3d6597a72f641a6,18),
    ('val-valeria','polygon','VAL',0x456f931298065b1852647de005dd27227146d8b9,18),
    ('truehn-true-human-nature','polygon','TRUEHN',0x9d9f8a6a6ad70d5670b7b5ca2042c7e106e2fb78,9),
    ('stg-stargatetoken','polygon','STG',0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590,18),
    ('titan-iron-titanium-token','polygon','IRON ',0xaaa5b9e6c589642f98a1cda99b9d024b8407285a,18),
    ('grt-the-graph','polygon','GRT',0x5fe2b58c013d7601147dcdd68c143a77499f5531,18),
    ('snx-synthetix-network-token','polygon','SNX',0x50b728d8d964fd00c2d0aad81718b71311fef68a,18),
    ('sff-sunflower-farm','polygon','SFF',0xdf9b4b57865b403e08c85568442f95c26b7896b0,18),
    ('dino-dinoswap','polygon','DINO',0xaa9654becca45b5bdfa5ac646c939c62b527d394,18),
    ('xend-xend-finance','polygon','XEND',0x86775d0b80b3df266af5377db34ba8f318d715ec,18),
    ('mst-idle-mystic','polygon','MST',0xa353deb6fb81df3844d8bd614d33d040fdbb8188,18),
    ('makerx-makerx','polygon','MAKERX',0x1ed02954d60ba14e26c230eec40cbac55fa3aeea,18),
    ('champ-ultimate-champions','polygon','CHAMP',0xed755dba6ec1eb520076cec051a582a6d81a8253,18),
    ('woo-wootrade','polygon','WOO',0x1b815d120b3ef02039ee11dc2d33de7aa4a8c603,18),
    ('route-router-protocol','polygon','ROUTE',0x16eccfdbb4ee1a85a33f3a9b21175cd7ae753db4,18),
    ('pla-playdapp','polygon','PLA',0x8765f05adce126d70bcdf1b0a48db573316662eb,18),
    ('comp-compoundd', 'polygon', 'COMP', 0x8505b9d2254a7ae468c0e9dd10ccea3a837aef5c,18),
    ('voxel-voxies','polygon','VOXEL',0xd0258a3fD00f38aa8090dfee343f10A9D4d30D3F,18),
    ('gtc-gitcoin', 'polygon', 'GTC', 0x3d93f3bc2cb79c31b4df652cd332d84d16317889,18),
    ('gmt-stepn', 'polygon', 'GMT', 0x714db550b574b3e927af3d93e26127d15721d4c2,8),
    ('tel-telcoin', 'polygon', 'TEL', 0xdf7837de1f2fa4631d716cf2502f8b230f1dcc32,2),
    ('mana-decentraland', 'polygon', 'MANA', 0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4,18),
    ('ata-automata', 'polygon', 'ATA', 0x0df0f72EE0e5c9B7ca761ECec42754992B2Da5BF, 18),
    ('bonk-bonk', 'polygon', 'BONK', 0xe5b49820e5a1063f6f4ddf851327b5e8b2301048, 5),
    ('rndr-render-token', 'polygon', 'RNDR', 0x61299774020da444af134c82fa83e3810b309991, 18),
    ('uni-uniswap', 'polygon', 'UNI', 0xb33eaad8d922b1083446dc23f610c2567fb5180f, 18),
    ('ape-apecoin', 'polygon', 'APE', 0xb7b31a6bc18e48888545ce79e83e06003be70930, 18),
    ('mkr-maker', 'polygon', 'MKR', 0x6f7c932e7684666c9fd1d44527765433e01ff61d, 18),
    ('cbeth-coinbase-wrapped-staked-eth', 'polygon', 'CBETH', 0x4b4327db1600b8b1440163f667e199cef35385f5, 18),
    ('busd-binance-usd', 'polygon', 'BUSD', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 18),
    ('reth-rocket-pool-eth', 'polygon', 'RETH', 0x0266f4f08d82372cf0fcbccc0ff74309089c74d1, 18),
    ('lusd-liquity-usd', 'polygon', 'LUSD', 0x23001f892c0c82b79303edc9b9033cd190bb21c7, 18),
    ('tusd-trueusd', 'polygon', 'TUSD', 0x2e1ad108ff1d8c782fcbbb89aad783ac49586756, 18),
    ('bat-basic-attention-token', 'polygon', 'BAT', 0x3cef98bb43d732e2f285ee605a8158cde967d219, 18),
    ('wavax-wrapped-avax-wormhole', 'polygon', 'WAVAX', 0x2c89bbc92bd86f8075d1decc58c7f4e0107f286b, 18),
    ('tbtc-tbtc', 'polygon', 'TBTC', 0x236aa50979d5f3de3bd1eeb40e81137f22ab794b, 18),
    ('ldo-lido-dao', 'polygon', 'LDO', 0xc3c7d422809852031b44ab29eec9f1eff2a58756, 18),
    ('egx-enegra', 'polygon', 'EGX', 0x8db0a6d1b06950b4e81c4f67d1289fc7b9359c7f, 6),
    --('bklay-orbit-bridge-bsc-klay', 'polygon', 'BKLAY', 0x0a02d33031917d836bd7af02f9f7f6c74d67805f,18), --inactive
    ('kek-kek', 'polygon', 'KEK', 0x42e5e06ef5b90fe15f853f59299fc96259209c5c,18),
    --('mesh-meshswap-protocol', 'polygon', 'MESH', 0x82362ec182db3cf7829014bc61e9be8a2e82868a,18), --inactive
    --('xdg-decentral-games-governance', 'polygon', 'XDG', 0xc6480da81151b2277761024599e8db2ad4c388c8,18), --inactive
    ('shib-shiba-inu-pos', 'polygon', 'SHIB', 0x6f8a06447ff6fcf75d803135a7de15ce88c1d4ec,18),
    ('pgx-pegaxy-stone', 'polygon', 'PGX', 0xc1c93d475dc82fe72dbc7074d55f5a734f8ceeae,18),
    ('klima-klimadao', 'polygon', 'KLIMA', 0x4e78011ce80ee02d2c3e649fb657e45898257815,9),
    ('bct-toucan-protocol-base-carbon-tonne', 'polygon', 'BCT', 0x2f800db0fdb5223b3c3f354886d907a671414a7f,18),
    ('mv-gensokishi-metaverse', 'polygon', 'MV', 0xa3c322ad15218fbfaed26ba7f616249f7705d945,18),
    ('rond-rond-coin', 'polygon', 'ROND', 0x204820b6e6feae805e376d2c6837446186e57981,18),
    ('elon-dogelon-mars', 'polygon', 'ELON', 0xe0339c80ffde91f3e20494df88d4206d86024cdf,18),
    ('qi-qidao', 'polygon', 'QI', 0x580a84c73811e1839f75d86d75d88cca0c241ff4,18),
    ('gmee-gamee', 'polygon', 'GMEE', 0xcf32822ff397ef82425153a9dcb726e5ff61dca7,18),
    --('mimatic-mai', 'polygon', 'miMATIC', 0xa3fa99a148fa48d14ed51d610c367c61876997f1,18), --not found in API
    ('fish-polycat-finance', 'polygon', 'FISH', 0x3a3df212b7aa91aa0402b9035b098891d276572b,18),
    --('pbos-phobos-token', 'polygon', 'PBOS', 0x421b9b487d5a9b76e4b81809c0f1b9bb8cb24cb9,18), --inactive
    ('pyr-vulcan-forged', 'polygon', 'PYR', 0x430ef9263e76dae63c84292c3409d61c598e9682,18),
    ('geod-geodnet-token', 'polygon', 'GEOD', 0xac0f66379a6d7801d7726d5a943356a172549adb,18),
    ('par-parallel', 'polygon', 'PAR', 0xe2aa7db6da1dae97c5f5c6914d285fbfcc32a128,18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
where contract_address not in (
    0xef938b6da8576a896f6e0321ef80996f4890f9c4 -- DG, bad price feed
    ,0xc6c855ad634dcdad23e64da71ba85b8c51e5ad7c -- ICE DG, bad price feed
    ,0x431cd3c9ac9fc73644bf68bf5691f4b83f9e104f -- RBW, bad price feed
    ,0x2ab0e9e4ee70fff1fb9d67031e44f6410170d00e -- XEN, bad price feed because of mXEN<>XEN
    ,0x311434160d7537be358930def317afb606c0d737 -- NAKA, bad price feed
)