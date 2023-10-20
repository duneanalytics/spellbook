{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_meta'),
    materialized = 'view'
  )
}}

{% set aave = 'Aave' %}
{% set arbichainlinkoracle = 'ArbiChainlinkOracle' %}
{% set cvioracle = 'CVIOracle' %}
{% set chainlink = 'Chainlink' %}
{% set chainlinkfeedoracle = 'ChainlinkFeedOracle' %}
{% set chainlinkfiatoracle = 'ChainlinkFiatOracle' %}
{% set chainlinkoracle = 'ChainlinkOracle' %}
{% set chainlinkoraclewrapper = 'ChainlinkOracleWrapper' %}
{% set chainlinkpriceoraclev1 = 'ChainlinkPriceOracleV1' %}
{% set chainlinkrateprovider = 'ChainlinkRateProvider' %}
{% set chainlinkv3priceprovider = 'ChainlinkV3PriceProvider' %}
{% set chainlinkwbtcusdcpriceoracleproxy = 'ChainlinkWBTCUSDCPriceOracleProxy' %}
{% set chainlinkwethusdcpriceoracleproxy = 'ChainlinkWETHUSDCPriceOracleProxy' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set ens = 'ENS' %}
{% set gnspriceaggregatorv6_3 = 'GNSPriceAggregatorV6_3' %}
{% set gnspriceaggregatorv6_4 = 'GNSPriceAggregatorV6_4' %}
{% set keeperregistry1_3 = 'KeeperRegistry1_3' %}
{% set leveloracle = 'LevelOracle' %}
{% set oracle = 'Oracle' %}
{% set perpetualfutures = 'PerpetualFutures' %}
{% set priceoracleproxyeth = 'PriceOracleProxyETH' %}
{% set priceoraclesentinel = 'PriceOracleSentinel' %}
{% set strategyhelper = 'StrategyHelper' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set unverified = 'Unverified' %}
{% set vrfcoordinatorv2 = 'VRFCoordinatorV2' %}
{% set vaultpricefeed = 'VaultPriceFeed' %}
{% set vaultpricefeedv21fast = 'VaultPriceFeedV21Fast' %}
{% set winroraclerouter = 'WINROracleRouter' %}
{% set wooraclev2 = 'WooracleV2' %}
{% set wooraclev2_1_zipinherit = 'WooracleV2_1_ZipInherit' %}

SELECT
   'arbitrum' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{aave}}', 0xc0ce5de939aad880b0bddcf9ab5750a53eda454b),
  ('{{aave}}', 0xb56c2f0b653b2e0b10c9b928c8580ac5df02c7c7),
  ('{{aave}}', 0xff785de8a851048a65cbe92c84d4167ef3ce9bac),
  ('{{arbichainlinkoracle}}', 0xecb0ab1b57bcda08d96e5580b034ba02b9de0ad8),
  ('{{arbichainlinkoracle}}', 0x94c105b8927333b7fa45d60418061d5a3b9a2faf),
  ('{{cvioracle}}', 0x649813b6dc6111d67484badedd377d32e4505f85),
  ('{{chainlink}}', 0xe73eccd1bbeed154d42731cd09552c6f2db42183),
  ('{{chainlink}}', 0x4ca8c060ebfbcf82111c5da3a2619a8c71b12c96),
  ('{{chainlinkfeedoracle}}', 0xe8ee3b7d74bd6eebba0992353b9fec66abf28dc8),
  ('{{chainlinkfiatoracle}}', 0x15fd4e8244ad590e2582a76c3e3098675fda9315),
  ('{{chainlinkoracle}}', 0x04b7a474fd142d19604de4b834d337cc94bdbb86),
  ('{{chainlinkoraclewrapper}}', 0xeceaea7e0408606714b2559ac9b1d3d51a327afe),
  ('{{chainlinkpriceoraclev1}}', 0xea3fe12d8cc2e87f99e985ee271971c808006531),
  ('{{chainlinkrateprovider}}', 0xf7c5c26b574063e7b098ed74fad6779e65e3f836),
  ('{{chainlinkv3priceprovider}}', 0x9d0dde842801448534263ff23c629edc6b6b31ee),
  ('{{chainlinkwbtcusdcpriceoracleproxy}}', 0x99155e68ac1523b6f461f6427a90607eccf7bdf5),
  ('{{chainlinkwethusdcpriceoracleproxy}}', 0x4777a6f28c8bb260d9a945dddefabb942ae10f1f),
  ('{{eacaggregatorproxy}}', 0x639fe6ab55c921f74e7fac1ee960c0b6293ba612),
  ('{{eacaggregatorproxy}}', 0x50834f3163758fcc1df9973b6e91f0f0f0434ad3),
  ('{{eacaggregatorproxy}}', 0x6ce185860a4963106506c203335a2910413708e9),
  ('{{eacaggregatorproxy}}', 0x20d0fcab0ecfd078b036b6caf1fac69a6453b352),
  ('{{eacaggregatorproxy}}', 0x3f3f5df88dc9f13eac63df89ec16ef6e7e25dde7),
  ('{{eacaggregatorproxy}}', 0x86e53cf1b870786351da77a57575e79cb55812cb),
  ('{{eacaggregatorproxy}}', 0xc5c8e77b397e531b8ec06bfb0048328b30e9ecfb),
  ('{{eacaggregatorproxy}}', 0x9c917083fdb403ab5adbec26ee294f6ecada2720),
  ('{{eacaggregatorproxy}}', 0xb2a824043730fe05f3da2efafa1cbbe83fa548d6),
  ('{{eacaggregatorproxy}}', 0xb1552c5e96b312d0bf8b554186f846c40614a540),
  ('{{eacaggregatorproxy}}', 0x0809e3d38d1b4214958faf06d8b1b1a2b73f2ab8),
  ('{{eacaggregatorproxy}}', 0x07c5b924399cc23c24a95c8743de4006a32b7f2a),
  ('{{eacaggregatorproxy}}', 0xfdb631f5ee196f0ed6faa767959853a9f217697d),
  ('{{eacaggregatorproxy}}', 0x87121f6c9a9f6e90e59591e4cf4804873f54a95b),
  ('{{eacaggregatorproxy}}', 0xb7c8fb1db45007f98a68da0588e1aa524c317f27),
  ('{{eacaggregatorproxy}}', 0xbcd8bea7831f392bb019ef3a672cc15866004536),
  ('{{eacaggregatorproxy}}', 0x24cea4b8ce57cda5058b924b9b9987992450590c),
  ('{{eacaggregatorproxy}}', 0xdb98056fecfff59d032ab628337a4887110df3db),
  ('{{eacaggregatorproxy}}', 0x52099d4523531f678dfc568a7b1e5038aadce1d6),
  ('{{eacaggregatorproxy}}', 0xc373b9db0707fd451bc56ba5e9b029ba26629df0),
  ('{{eacaggregatorproxy}}', 0xaebda2c976cfd1ee1977eac079b4382acb849325),
  ('{{eacaggregatorproxy}}', 0x9a7fb1b3950837a8d9b40517626e11d4127c098c),
  ('{{ens}}', 0x44b1daf988e0c5d1f9bdca0e1d1c89f462c9a5dc),
  ('{{ens}}', 0x94c917ee4c7c3d0de0b71beec863c535526f0438),
  ('{{ens}}', 0xaa1f464df38cec1404ec065dfb8a071813fae073),
  ('{{gnspriceaggregatorv6_3}}', 0xcef1c791cdd8c3ea92d6ab32399119fd30e1ff21),
  ('{{gnspriceaggregatorv6_4}}', 0x2e44a81701a8355e59b3204b4a9fe8fc43cbe0c3),
  ('{{keeperregistry1_3}}', 0x75c0530885f385721fdda23c539af3701d6183d4),
  ('{{leveloracle}}', 0x82b585a8f15701bbd671850f0a9f1fee57a8dcb5),
  ('{{leveloracle}}', 0x1e56ab83ac0cb52713762e48915fb368ec99baa1),
  ('{{oracle}}', 0xa11b501c2dd83acd29f6727570f2502faaa617f2),
  ('{{oracle}}', 0x9f5982374e63e5b011317451a424be9e1275a03f),
  ('{{oracle}}', 0xb4e0e46cc733106f8f5b9845e2011b128a1ea39a),
  ('{{perpetualfutures}}', 0x32ef81d6b048df4b080999b0a5c4f70a2881434f),
  ('{{priceoracleproxyeth}}', 0xb7723c5259b287e94fb42bdbdf2c190cdeaf1072),
  ('{{priceoracleproxyeth}}', 0xccf9393df2f656262fd79599175950fab4d4ec01),
  ('{{priceoraclesentinel}}', 0xf876d26041a4fdc7a787d209dc3d2795ddc74f1e),
  ('{{strategyhelper}}', 0x72f7101371201cefd43af026eef1403652f115ee),
  ('{{transparentupgradeableproxy}}', 0x2ed49363057aa5be5ea133d8fe8c9c9cd488d3d2),
  ('{{transparentupgradeableproxy}}', 0x76663727c39dd46fed5414d6801c4e8890df85cf),
  ('{{transparentupgradeableproxy}}', 0xeae2864a02818b749a10f4b86257e514b7044e98),
  ('{{transparentupgradeableproxy}}', 0x2e2e643eb601ca3b36ddb948cdddb8914b6f41fc),
  ('{{transparentupgradeableproxy}}', 0x3e0199792ce69dc29a0a36146bfa68bd7c8d6633),
  ('{{transparentupgradeableproxy}}', 0x9e9c2701d0cab33e70f0e3ccc32545fdab7af3d4),
  ('{{transparentupgradeableproxy}}', 0x26f811ce7c3a567e192b4dc43c0c5399632a95fe),
  ('{{transparentupgradeableproxy}}', 0x7d135662818d3540bd6f23294bfdb6946c52c9ab),
  ('{{transparentupgradeableproxy}}', 0x866f9cd6b82c32f1842c164e47c6dafc6f0a68f4),
  ('{{transparentupgradeableproxy}}', 0x4c0fd090878ba3bb412301bdd380e1b37122bc0d),
  ('{{transparentupgradeableproxy}}', 0x20151ff7fdd720b85063d02081aa5b7876adff7b),
  ('{{transparentupgradeableproxy}}', 0x563ccabfbaccb1a2e00d21704570cfc1af21f47f),
  ('{{transparentupgradeableproxy}}', 0xf3f98086f7b61a32be4edf8d8a4b964ec886bbcd),
  ('{{transparentupgradeableproxy}}', 0x458abbf3d1774d8d80f4abe9960d091aa3ee1283),
  ('{{transparentupgradeableproxy}}', 0x351c0c11aea15b1036589c93dfb9a491cdc9264b),
  ('{{transparentupgradeableproxy}}', 0x54d8864e8855a7b66ee42b8f2eaa0f2e06bd641a),
  ('{{transparentupgradeableproxy}}', 0x8991c4c347420e476f1cf09c03aba224a76e2997),
  ('{{unverified}}', 0x849b45d86c24bf8d33bc3cf7c6e37e9f83e2351f),
  ('{{unverified}}', 0x511b79a6e0c6d07eb23bb735c5dbe901c2b54b7f),
  ('{{unverified}}', 0xbcdb9b040abf11d561b20c78e38130a8ffeb0364),
  ('{{unverified}}', 0x66ceb1e152979953ade3e9e2e734f8f4951ad1c8),
  ('{{unverified}}', 0x8a4236f5ef6158546c34bd7bc2908b8106ab1ea1),
  ('{{unverified}}', 0x5b1e0f4883bc0c8c0cc52a289460d6936e7df719),
  ('{{unverified}}', 0x90ff4f5d03f686bfe346041e6e1ab8402551bc3b),
  ('{{unverified}}', 0x1441b99da7854a304133630048dc6cf43580b1af),
  ('{{unverified}}', 0xf73ab2d782bf6ba97ac4405d2cd4f1135da8dbd9),
  ('{{unverified}}', 0xc9dc823040f0100a1e78d47d08c7de6dd8b9b064),
  ('{{unverified}}', 0x150a58e9e6bf69cceb1dba5ae97c166dc8792539),
  ('{{unverified}}', 0x8215102019c38735f8279fd0e9abbd3cc9f60d9a),
  ('{{unverified}}', 0x14f7e8e31b794aa9674f2f861ef45d9081ab827e),
  ('{{vrfcoordinatorv2}}', 0x41034678d6c633d8a95c75e1138a360a28ba15d1),
  ('{{vaultpricefeed}}', 0x2d68011bca022ed0e474264145f46cc4de96a002),
  ('{{vaultpricefeed}}', 0xfe661cbf27da0656b7a1151a761ff194849c387a),
  ('{{vaultpricefeed}}', 0xa18bb1003686d0854ef989bb936211c59eb6e363),
  ('{{vaultpricefeed}}', 0xda99e5aa0c43ca209356e0dce7edd7fbb1dbb703),
  ('{{vaultpricefeed}}', 0xeff37c0969dcbf69b0b142dac4e56a0930aecba8),
  ('{{vaultpricefeed}}', 0xdffe2f49234001380119d89e9c965bed4bf123f0),
  ('{{vaultpricefeed}}', 0x2c96437d4da023b5c65a7e20124c3cd7b7fd4fb8),
  ('{{vaultpricefeed}}', 0x1cf4579904eb2acda0e4081e39ec10d0c32b5de3),
  ('{{vaultpricefeed}}', 0x7b9e962dd8aed0db9a1d8a2d7a962ad8b871ce4f),
  ('{{vaultpricefeed}}', 0xf28e261b89fc4479ee41044dd55f7a4053f9844a),
  ('{{vaultpricefeedv21fast}}', 0x046600975bed388d368f843a67e41545e27a2591),
  ('{{winroraclerouter}}', 0xced8affd816e8e0420aeb15a798b5f443bc8dd0e),
  ('{{wooraclev2}}', 0x37a9de70b6734dfca54395d8061d9411d9910739),
  ('{{wooraclev2_1_zipinherit}}', 0x73504eacb100c7576146618dc306c97454cb3620)
) a (requester_name, requester_address)
