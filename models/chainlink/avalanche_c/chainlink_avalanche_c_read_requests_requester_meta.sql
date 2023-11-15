{{
  config(
    
    alias='read_requests_requester_meta',
    materialized = 'view'
  )
}}

{% set aave = 'Aave' %}
{% set adminupgradeabilityproxy = 'AdminUpgradeabilityProxy' %}
{% set aggregatorproxy = 'AggregatorProxy' %}
{% set benqichainlinkoracle = 'BenqiChainlinkOracle' %}
{% set binarybet = 'BinaryBet' %}
{% set chainlinkoracle = 'ChainlinkOracle' %}
{% set chainlinkoraclev2 = 'ChainlinkOracleV2' %}
{% set chainlinkpriceoracle = 'ChainlinkPriceOracle' %}
{% set chainlinkproxypriceprovider = 'ChainlinkProxyPriceProvider' %}
{% set cointoss = 'CoinToss' %}
{% set computeapr = 'ComputeAPR' %}
{% set dice = 'Dice' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set keeperregistry = 'KeeperRegistry' %}
{% set linkoracle = 'LinkOracle' %}
{% set masterofcoin = 'MasterOfCoin' %}
{% set oftwrapper = 'OFTWrapper' %}
{% set optimizedtransparentupgradeableproxy = 'OptimizedTransparentUpgradeableProxy' %}
{% set oracle = 'Oracle' %}
{% set oraclemanagerchainlink = 'OracleManagerChainlink' %}
{% set oraclerouter = 'OracleRouter' %}
{% set pricefeed = 'PriceFeed' %}
{% set priceoracleproxyusd = 'PriceOracleProxyUSD' %}
{% set pricingoraclev1 = 'PricingOracleV1' %}
{% set roulette = 'Roulette' %}
{% set sofrrateoracle = 'SofrRateOracle' %}
{% set timebonddepository = 'TimeBondDepository' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set unverified = 'Unverified' %}
{% set vrfcoordinatorv2 = 'VRFCoordinatorV2' %}
{% set vaultpricefeed = 'VaultPriceFeed' %}
{% set wooraclev2 = 'WooracleV2' %}
{% set wooraclev2_1 = 'WooracleV2_1' %}
{% set avaxchainlinkoracle = 'avaxChainLinkOracle' %}
{% set crosschainqistablecoin = 'crosschainQiStablecoin' %}
{% set mooraclesingle = 'mooRacleSingle' %}
{% set savaxoracleadapter = 'sAVAXOracleAdapter' %}
{% set wmemooracle = 'wMemoOracle' %}

SELECT
   'avalanche_c' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{aave}}', 0xdc336cd4769f4cc7e9d726da53e6d3fc710ceb89),
  ('{{aave}}', 0xebd36016b3ed09d4693ed4251c67bd858c3c7c9c),
  ('{{aave}}', 0x89fc4fa08b5fcb8fa9538d6cc25b638370fc26d8),
  ('{{aave}}', 0x26d5477ecc064d075df6033704fb524a529e8b46),
  ('{{adminupgradeabilityproxy}}', 0x11f6384dd7c5ca1e9d5ea15147a1c445b73d5cf4),
  ('{{aggregatorproxy}}', 0x2d18bf74cd2cc1e2e7e239768e27d74748bc31b6),
  ('{{benqichainlinkoracle}}', 0x316ae55ec59e0beb2121c0e41d4bdef8bf66b32b),
  ('{{binarybet}}', 0xd582aeaa7202a8d68a6c31db92e53c16019e1740),
  ('{{chainlinkoracle}}', 0x3dc65b58ad56309349f9494ed8d920870aaa8c5b),
  ('{{chainlinkoraclev2}}', 0x04bcda3c65b2f28ade0a40e9f2691681f531d20e),
  ('{{chainlinkpriceoracle}}', 0xab8fad22bb7d026f3f2d40bcc58c8644fb2763da),
  ('{{chainlinkproxypriceprovider}}', 0x7b52f4b5c476e7afd09266c35274737cd0af746b),
  ('{{cointoss}}', 0x272b4dd8e842be1012c9ccbb89b636996f9bae98),
  ('{{computeapr}}', 0x444af9d53d489c5521bdf9719c88838372424831),
  ('{{dice}}', 0x8d940a4859570754d364b86ccc05052ea5068ea6),
  ('{{eacaggregatorproxy}}', 0x0a77230d17318075983913bc2145db16c7366156),
  ('{{eacaggregatorproxy}}', 0xf096872672f44d6eba71458d74fe67f9a77a23b9),
  ('{{eacaggregatorproxy}}', 0x2779d32d5166baaa2b2b658333ba7e6ec0c65743),
  ('{{eacaggregatorproxy}}', 0x976b3d034e162d8bd72d6b9c989d545b839003b0),
  ('{{eacaggregatorproxy}}', 0xebe676ee90fe1112671f19b6b7459bc678b67e8a),
  ('{{eacaggregatorproxy}}', 0x51d7180eda2260cc4f6e4eebb82fef5c3c2b8300),
  ('{{eacaggregatorproxy}}', 0x1b8a25f73c9420dd507406c3a3816a276b62f56a),
  ('{{eacaggregatorproxy}}', 0x54edab30a7134a16a54218ae64c73e1daf48a8fb),
  ('{{eacaggregatorproxy}}', 0x49ccd9ca821efeab2b98c60dc60f518e765ede9a),
  ('{{eacaggregatorproxy}}', 0x36e039e6391a5e7a7267650979fdf613f659be5d),
  ('{{eacaggregatorproxy}}', 0x02d35d3a8ac3e1626d3ee09a78dd87286f5e8e3a),
  ('{{eacaggregatorproxy}}', 0x2854ca10a54800e15a2a25cfa52567166434ff0a),
  ('{{eacaggregatorproxy}}', 0xbba56ef1565354217a3353a466edb82e8f25b08e),
  ('{{eacaggregatorproxy}}', 0x3ca13391e9fb38a75330fb28f8cc2eb3d9ceceed),
  ('{{eacaggregatorproxy}}', 0xd1cc11c5102be7dd8919715e6b04e1af1e43fdc4),
  ('{{eacaggregatorproxy}}', 0x827f8a0dc5c943f7524dda178e2e7f275aad743f),
  ('{{eacaggregatorproxy}}', 0x86442e3a98558357d46e6182f4b262f76c4fa26f),
  ('{{eacaggregatorproxy}}', 0xf58b78581c480caff667c63fedd564ecf01ef86b),
  ('{{eacaggregatorproxy}}', 0x4f3ddf9378a4865cf4f28be51e10aecb83b7daee),
  ('{{eacaggregatorproxy}}', 0x7cf8a6090a9053b01f3df4d4e6cfedd8c90d9027),
  ('{{eacaggregatorproxy}}', 0x192f2dba961bb0277520c082d6bfa87d5961333e),
  ('{{keeperregistry}}', 0x02777053d6764996e594c3e88af1d58d5363a2e6),
  ('{{keeperregistry}}', 0x409cf388dab66275da3e44005d182c12eeaa12a0),
  ('{{linkoracle}}', 0xd4d7bcf6c7b54349c91f39cad89b228c53fe6bd7),
  ('{{masterofcoin}}', 0x0d8c4c9a47a6eb59e3983ee69cc1fd4907554cb2),
  ('{{oftwrapper}}', 0x287176dfbec7e8cee0f876fc7b52960ee1784adc),
  ('{{optimizedtransparentupgradeableproxy}}', 0x150a58e9e6bf69cceb1dba5ae97c166dc8792539),
  ('{{oracle}}', 0x62e1c8f56c7de5eb5adf313e97c4bbb4e7fd956b),
  ('{{oraclemanagerchainlink}}', 0xec120de9ffae289f5b383ffb582f3cc1f449e3aa),
  ('{{oraclemanagerchainlink}}', 0xe22268db718912dc6a73106dd4abf34080b1c4e1),
  ('{{oraclemanagerchainlink}}', 0x9341437bbb9c7c0ed5dcada60886780ab3c81524),
  ('{{oraclerouter}}', 0xdc7be21354c9efcf10cd2ec99e7315eec40db060),
  ('{{pricefeed}}', 0x8dbdf36bd8f605d698f6019f9b458e67a0395c94),
  ('{{pricefeed}}', 0xadf6a71ee66edfc62bc6d35016acd6fc78687863),
  ('{{pricefeed}}', 0x45f7260f7cc47b15eb5cb6ac0daabd8efb2a0edb),
  ('{{pricefeed}}', 0xd3c41547dc70baff35f73c0b7dfe10e83ba59e68),
  ('{{pricefeed}}', 0x81b23b288e6cdd27007394e1c1a7645cb58239f1),
  ('{{pricefeed}}', 0xc0d7e95fbc3181e2d38fb2518214e1186adb45dc),
  ('{{pricefeed}}', 0x5afa0dd83b1511408299ebbe4aec3c1eb7625ba8),
  ('{{pricefeed}}', 0x5e22d379e7bca7cefe9ad147007fd5b4cf97432f),
  ('{{pricefeed}}', 0x2d23f118d399abdbd3d801dd78460bcb4434d070),
  ('{{pricefeed}}', 0xa7debc6a5129dbdbd9bde55769ae603a7df0d41d),
  ('{{pricefeed}}', 0x1465528ea599cc9fb5268cfa94c3fab21029a8de),
  ('{{pricefeed}}', 0xcd4dd92cf059f5d8464020a03ecb5bd0ad8f0a9b),
  ('{{pricefeed}}', 0x1541623a6d07453336fb41211569d178d44d7800),
  ('{{pricefeed}}', 0x374b67fecfe060664812c03a90d6a0130095dd96),
  ('{{pricefeed}}', 0x7b1e31530b00d1b1aa94cce05a96238242da5dbd),
  ('{{priceoracleproxyusd}}', 0xd7ae651985a871c1bc254748c40ecc733110bc2e),
  ('{{priceoracleproxyusd}}', 0xe34309613b061545d42c4160ec4d64240b114482),
  ('{{priceoracleproxyusd}}', 0x0980f2f0d2af35ef2c4521b2342d59db575303f7),
  ('{{pricingoraclev1}}', 0x5a755af3650179d02a93f37220caf76a34d8d975),
  ('{{roulette}}', 0x556bc2bbe113cec48fc3b2c45afb708fb09dda63),
  ('{{sofrrateoracle}}', 0x54b868b03c68a1307b24fb0a4b60b18a0714a94c),
  ('{{timebonddepository}}', 0xe02b1aa2c4be73093be79d763fdffc0e3cf67318),
  ('{{timebonddepository}}', 0x858636f350fc812c3c88d1578925c502727ab323),
  ('{{transparentupgradeableproxy}}', 0x7511e2ccae82cdab12d51f0d1519ad5450f157de),
  ('{{transparentupgradeableproxy}}', 0x0ba2e492e8427fad51692ee8958ebf936bee1d84),
  ('{{transparentupgradeableproxy}}', 0xa09944e84e12ba0e9dd5dd5c0ead45a8851fb280),
  ('{{transparentupgradeableproxy}}', 0x47fdf3fb0c2b51b5e8fad4d4278e7c592e646c97),
  ('{{transparentupgradeableproxy}}', 0x9adcbba4b79ee5285e891512b44706f41f14cafd),
  ('{{transparentupgradeableproxy}}', 0x9ff243cd6564abc6eb9d52d0df923f3613c351d2),
  ('{{unverified}}', 0x9ad4df248019a36b29202aec91a1b9aaa863341a),
  ('{{unverified}}', 0x5fc20fcd1b50c5e1196ac790dadcfcdd416bb0c7),
  ('{{unverified}}', 0x8b312f4503790cbd1030b97c545c7f3efdade717),
  ('{{unverified}}', 0xfa3c08b2ddb96ba0063bb64f1ea7dc81342f06cc),
  ('{{unverified}}', 0x404444d45e7d3f3318e85a98500ccc3aa039ae40),
  ('{{unverified}}', 0xd35000f3ffc3cd151d8ce4d85f5f988303a498bf),
  ('{{unverified}}', 0x0824545b22dd6dc644c8b66d7923e613816ff63a),
  ('{{unverified}}', 0x7eb63775c4df013c7474b933559d5e5291eb1a2f),
  ('{{unverified}}', 0xe12feebc0a519708dcd573b41e91753225a8c271),
  ('{{unverified}}', 0xdb09f3a7ed15b2db42c81f2fbfe8251a7622dbef),
  ('{{unverified}}', 0x1fd7eec79f2e7eba8a563f638d60d4ddf58c93f6),
  ('{{vrfcoordinatorv2}}', 0xd5d517abe5cf79b7e95ec98db0f0277788aff634),
  ('{{vaultpricefeed}}', 0x27e99387af40e5ca9ce21418552f15f02c8c57e7),
  ('{{vaultpricefeed}}', 0x81b7e71a1d9e08a6ca016a0f4d6fa50dbce89ee3),
  ('{{vaultpricefeed}}', 0x205646b93b9d8070e15bc113449586875ed7288e),
  ('{{vaultpricefeed}}', 0x131238112aa25c0d8cd237a6c384d1a86d2bb152),
  ('{{vaultpricefeed}}', 0x7bcda4bcf049d3b1e175d81bb847b03c52e8e1ce),
  ('{{wooraclev2}}', 0x9aca557590f5020bda4ba63065fc3a1253bf8000),
  ('{{wooraclev2_1}}', 0xc13843ae0d2c5ca9e0efb93a78828446d8173d19),
  ('{{avaxchainlinkoracle}}', 0x4954f1e66d65df727a87c9f29f17bf0672ad8dbf),
  ('{{crosschainqistablecoin}}', 0xfa19c1d104f4aefb8d5564f02b3adca1b515da58),
  ('{{mooraclesingle}}', 0xddc3d26baa9d2d979f5e2e42515478bf18f354d5),
  ('{{savaxoracleadapter}}', 0xc9245871d69bf4c36c6f2d15e0d68ffa883fe1a7),
  ('{{wmemooracle}}', 0xc9facfa2fc50c9a30c77a2ad14e2db107d591918)
) a (requester_name, requester_address)
