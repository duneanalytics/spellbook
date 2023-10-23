{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_meta'),
    materialized = 'view'
  )
}}

{% set aave = 'Aave' %}
{% set chainlinkpriceoraclev2 = 'ChainlinkPriceOracleV2' %}
{% set coinflip = 'CoinFlip' %}
{% set collateraloracle = 'CollateralOracle' %}
{% set curve = 'Curve' %}
{% set dice = 'Dice' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set eurpricefeed = 'EurPriceFeed' %}
{% set gfarmpriceaggregatorv4 = 'GFarmPriceAggregatorV4' %}
{% set gfarmpriceaggregatorv5 = 'GFarmPriceAggregatorV5' %}
{% set gnspriceaggregatorv6 = 'GNSPriceAggregatorV6' %}
{% set gnspriceaggregatorv6_2 = 'GNSPriceAggregatorV6_2' %}
{% set gnspriceaggregatorv6_3 = 'GNSPriceAggregatorV6_3' %}
{% set gnspriceaggregatorv6_4 = 'GNSPriceAggregatorV6_4' %}
{% set ironpriceoracle = 'IronPriceOracle' %}
{% set keeperregistry = 'KeeperRegistry' %}
{% set nzdstousdassimilator = 'NzdsToUsdAssimilator' %}
{% set oraclemanagerchainlink = 'OracleManagerChainlink' %}
{% set oraclemanagerflipp3ning = 'OracleManagerFlipp3ning' %}
{% set oraclerouter = 'OracleRouter' %}
{% set ovixchainlinkoraclev2 = 'OvixChainlinkOracleV2' %}
{% set pricecalculator = 'PriceCalculator' %}
{% set pricefeeds_polygon = 'PriceFeeds_POLYGON' %}
{% set pricemanager = 'PriceManager' %}
{% set priceoracleproxyusd = 'PriceOracleProxyUSD' %}
{% set pricereporter = 'PriceReporter' %}
{% set pricesourcehybridsd3crv = 'PriceSourceHybridSd3crv' %}
{% set qistablecoin = 'QiStablecoin' %}
{% set shareoracle = 'ShareOracle' %}
{% set simplepriceoracle = 'SimplePriceOracle' %}
{% set swaap_pool = 'Swaap Pool' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set unverified = 'Unverified' %}
{% set usdctousdassimilator = 'UsdcToUsdAssimilator' %}
{% set vrfcoordinatorv2 = 'VRFCoordinatorV2' %}
{% set vaultpricefeed = 'VaultPriceFeed' %}
{% set wooguardian = 'WooGuardian' %}
{% set wooraclev2 = 'WooracleV2' %}
{% set wooraclev2_1 = 'WooracleV2_1' %}
{% set crosschainqistablecoinslimv2 = 'crosschainQiStablecoinSlimV2' %}
{% set doubleoracle = 'doubleOracle' %}
{% set erc20qistablecoin = 'erc20QiStablecoin' %}
{% set shareoracle = 'shareOracle' %}

SELECT
   'polygon' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{aave}}', 0x0229f777b0fab107f9591a41d5f02e4e98db6f2d),
  ('{{aave}}', 0xb023e699f5a33916ea823a16485e259257ca8bd1),
  ('{{chainlinkpriceoraclev2}}', 0x00cb8686a4ddcb60565f8a0f1a10307ec1d91b58),
  ('{{coinflip}}', 0x658d831192bf5008e89ab57b373d8c8c7e6f480e),
  ('{{collateraloracle}}', 0x785808779131b0947f42b4b54537a4682ebeab86),
  ('{{curve}}', 0x931d6a6cc3f992beee80a1a14a6530d34104b000),
  ('{{dice}}', 0xa45abc5a7f236b93809bb3228dd6e0b267b26fc4),
  ('{{eacaggregatorproxy}}', 0x5787befdc0ecd210dfa948264631cd53e68f7802),
  ('{{eacaggregatorproxy}}', 0xefb7e6be8356ccc6827799b6a7348ee674a80eae),
  ('{{eacaggregatorproxy}}', 0x327e23a4855b6f663a28c5161541d69af8973302),
  ('{{eacaggregatorproxy}}', 0xfe4a8cc5b5b2366c1b58bea3858e81843581b2f7),
  ('{{eacaggregatorproxy}}', 0xab594600376ec9fd91f8e885dadf0ce036862de0),
  ('{{eacaggregatorproxy}}', 0xf9680d99d6c9589e2a93a78a04a279e509205945),
  ('{{eacaggregatorproxy}}', 0xa338e0492b2f944e9f8c0653d3ad1484f2657a37),
  ('{{eacaggregatorproxy}}', 0xc907e116054ad103354f2d350fd2514433d57f6f),
  ('{{eacaggregatorproxy}}', 0xfc539a559e170f848323e19dfd66007520510085),
  ('{{eacaggregatorproxy}}', 0xf9d5aac6e5572aefa6bd64108ff86a222f69b64d),
  ('{{eacaggregatorproxy}}', 0xd9ffdb71ebe7496cc440152d43986aae0ab76665),
  ('{{eacaggregatorproxy}}', 0x0a6513e40db6eb1b165753ad52e80663aea50545),
  ('{{eacaggregatorproxy}}', 0x4746dec9e833a82ec7c2c1356372ccf2cfcd2f3d),
  ('{{eacaggregatorproxy}}', 0xbe23a3aa13038cfc28afd0ece4fde379fe7fbfc4),
  ('{{eacaggregatorproxy}}', 0x73366fe0aa0ded304479862808e02506fe556a98),
  ('{{eacaggregatorproxy}}', 0x72484b12719e23115761d5da1646945632979bb6),
  ('{{eacaggregatorproxy}}', 0xf824ea79774e8698e6c6d156c60ab054794c9b18),
  ('{{eacaggregatorproxy}}', 0xde31f8bfbd8c84b5360cfacca3539b938dd78ae6),
  ('{{eacaggregatorproxy}}', 0x336584c8e6dc19637a5b36206b1c79923111b405),
  ('{{eacaggregatorproxy}}', 0xa302a0b8a499fd0f00449df0a490dede21105955),
  ('{{eacaggregatorproxy}}', 0x97371df4492605486e23da797fa68e55fc38a13f),
  ('{{eacaggregatorproxy}}', 0x10c8264c0935b3b9870013e057f330ff3e9c56dc),
  ('{{eacaggregatorproxy}}', 0xb77fa460604b9c6435a235d057f7d319ac83cb53),
  ('{{eacaggregatorproxy}}', 0xdf0fb4e4f928d2dcb76f438575fdd8682386e13c),
  ('{{eacaggregatorproxy}}', 0xe638249af9642cda55a92245525268482ee4c67b),
  ('{{eacaggregatorproxy}}', 0xdd229ce42f11d8ee7fff29bdb71c7b81352e11be),
  ('{{eacaggregatorproxy}}', 0x1cf68c76803c9a415be301f50e82e44c64b7f1d4),
  ('{{eacaggregatorproxy}}', 0xbaf9327b6564454f4a3364c33efeef032b4b4444),
  ('{{eacaggregatorproxy}}', 0x49b0c695039243bbfeb8ecd054eb70061fd54aa0),
  ('{{eacaggregatorproxy}}', 0x1248573d9b62ac86a3ca02abc6abe6d403cd1034),
  ('{{eacaggregatorproxy}}', 0x882554df528115a743c4537828da8d5b58e52544),
  ('{{eacaggregatorproxy}}', 0x03cd157746c61f44597dd54c6f6702105258c722),
  ('{{eacaggregatorproxy}}', 0xd106b538f2a868c28ca1ec7e298c3325e0251d66),
  ('{{eacaggregatorproxy}}', 0xc9ecf45956f576681bdc01f79602a79bc2667b0c),
  ('{{eacaggregatorproxy}}', 0xacb51f1a83922632ca02b25a8164c10748001bde),
  ('{{eacaggregatorproxy}}', 0x82a6c4af830caa6c97bb504425f6a66165c2c26e),
  ('{{eurpricefeed}}', 0x247d082cd7649098d377ff8812d84aa354b55c14),
  ('{{gfarmpriceaggregatorv4}}', 0xfb694dbaa0f78a8b07be4a1a92ae12d2845138fb),
  ('{{gfarmpriceaggregatorv5}}', 0x8aaaff79c94debf411d4a4166c1aeab7a63ebeec),
  ('{{gnspriceaggregatorv6}}', 0x5fb47355828c0902acbbe759cee1b8342c41178b),
  ('{{gnspriceaggregatorv6_2}}', 0xa7a80445511c7b6685e0235a7ab61bd96349dd67),
  ('{{gnspriceaggregatorv6_3}}', 0x631e885028e75fcbb34c06d8ecb8e20ea18f6632),
  ('{{gnspriceaggregatorv6_4}}', 0x126f32723c5fc8dfeb17c46b7b7dd3dcd458a816),
  ('{{ironpriceoracle}}', 0x2572ac57821501c33e0750eba89e9f84b43fc775),
  ('{{keeperregistry}}', 0x02777053d6764996e594c3e88af1d58d5363a2e6),
  ('{{keeperregistry}}', 0x7b3ec232b08bd7b4b3305be0c044d907b2df960b),
  ('{{nzdstousdassimilator}}', 0xc37d72cd56f2ca25d2239ec0a3ea20e7187d185a),
  ('{{oraclemanagerchainlink}}', 0x063fd075441de59ccf0d941fa0500cb0b95db0c6),
  ('{{oraclemanagerflipp3ning}}', 0x70a760acd5503a8d6746bc7f00571f570ae0ad44),
  ('{{oraclerouter}}', 0x30c950fc01089ab69da176a6d6e8524311fd9e0f),
  ('{{ovixchainlinkoraclev2}}', 0x1c312b14c129eabc4796b0165a2c470b659e5f01),
  ('{{pricecalculator}}', 0xac2f66971bc37eb443c4e11aab277e19d1c4864c),
  ('{{pricefeeds_polygon}}', 0x600f8e7b10cf6da18871ff79e4a61b13caced9bc),
  ('{{pricemanager}}', 0x637c0d4525a2b0e247093aad1b5ef91631b47e3c),
  ('{{priceoracleproxyusd}}', 0x812c0b2a2a0a74f6f6ed620fbd2b67fec7db2190),
  ('{{pricereporter}}', 0xdcf15837c941012bf149d1eeed5c392be5fd213f),
  ('{{pricesourcehybridsd3crv}}', 0x56ca9b7a4db485606479def2b5eb70ecfac62e2c),
  ('{{qistablecoin}}', 0xa3fa99a148fa48d14ed51d610c367c61876997f1),
  ('{{shareoracle}}', 0x98843b46ee43ed11667ebd424306616cc31bdf4b),
  ('{{simplepriceoracle}}', 0x86de572881b19bc86e19bc3e0d01f80a035265d8),
  ('{{swaap_pool}}', 0x7f5f7411c2c7ec60e2db946abbe7dc354254870b),
  ('{{transparentupgradeableproxy}}', 0xe3b11c3bd6d90cfebbb4fb9d59486b0381d38021),
  ('{{transparentupgradeableproxy}}', 0x760fe3179c8491f4b75b21a81f3ee4a5d616a28a),
  ('{{transparentupgradeableproxy}}', 0x17fdec808166aac963cf182a1c175d753a44b9bf),
  ('{{unverified}}', 0x45ae0b1f75d1e053df943fea711dafb08afd10df),
  ('{{unverified}}', 0x4fac302cda8bfb21d4e5d667c3669bc8992fab28),
  ('{{unverified}}', 0x4ed9beb8ee5987c1273d5fd3830e8d0d0ff1f526),
  ('{{unverified}}', 0xfde9c3fddf28d3ec820eee3a878ca966c06d4602),
  ('{{unverified}}', 0x6496618f2979299a1288f05eb9719af0b0d1851d),
  ('{{unverified}}', 0xd1defb4a2c0c0c1b458d9cfbfce905d6f1231b80),
  ('{{unverified}}', 0x025368f3d99a2d25dce97a26b1d5ec9c21c1c31c),
  ('{{unverified}}', 0x47a4eb1a75abdb4f9fd1ae8ef11945f3b352e01f),
  ('{{unverified}}', 0xfbf07b6aa0c6df0e9a671e6447ad40c3de1188fe),
  ('{{unverified}}', 0xf73ab2d782bf6ba97ac4405d2cd4f1135da8dbd9),
  ('{{unverified}}', 0x85fad2de0701f90d08a16a8f8c66ea6490715e99),
  ('{{unverified}}', 0xa7f2eb138aa4aa46d1ce37dd688f25b642141333),
  ('{{unverified}}', 0xb3f313451d145d4ce1a1926d6f5330ca18dc8103),
  ('{{usdctousdassimilator}}', 0x8f022c3e9f8f915fd99c0e307059acd2b908d8e1),
  ('{{vrfcoordinatorv2}}', 0xae975071be8f8ee67addbc1a82488f1c24858067),
  ('{{vrfcoordinatorv2}}', 0x62581743bb71f297f4e782b5475b9f06308c58e8),
  ('{{vaultpricefeed}}', 0x133f4d5e703d68eef3ea22037c410f042c1642b2),
  ('{{vaultpricefeed}}', 0x6ac3162c11e74e759c6c57f3404fe5029c4288a6),
  ('{{vaultpricefeed}}', 0xb022b0353fe4c4af6fb3f5b1243a8da8a12e7c42),
  ('{{wooguardian}}', 0xf5d215d9c84778f85746d15762daf39b9e83a2d6),
  ('{{wooraclev2}}', 0xeff23b4be1091b53205e35f3afcd9c7182bf3062),
  ('{{wooraclev2_1}}', 0x31ae608cbadd1214d6a3d5dcf49e45fb18e2a48e),
  ('{{crosschainqistablecoinslimv2}}', 0x178f1c95c85fe7221c7a6a3d6f12b7da3253eeae),
  ('{{doubleoracle}}', 0xd56e523d18a205f96ca8009f2c85aa47f3fa35ab),
  ('{{erc20qistablecoin}}', 0x88d84a85a87ed12b8f098e8953b322ff789fcd1a),
  ('{{erc20qistablecoin}}', 0x3fd939b017b31eaadf9ae50c7ff7fa5c0661d47c),
  ('{{erc20qistablecoin}}', 0x11a33631a5b5349af3f165d2b7901a4d67e561ad),
  ('{{shareoracle}}', 0x7791b9d71fa3a9782183b810f26b5c2eedf53eb0),
  ('{{shareoracle}}', 0x0fda41a1d4555b85021312b765c6d519b9c66f93)
) a (requester_name, requester_address)
