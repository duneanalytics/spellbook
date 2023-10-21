{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_meta'),
    materialized = 'view'
  )
}}

{% set adminupgradeabilityproxy = 'AdminUpgradeabilityProxy' %}
{% set alpaca_finance = 'Alpaca Finance' %}
{% set annex_finance = 'Annex Finance' %}
{% set aperocket = 'ApeRocket' %}
{% set apollox = 'ApolloX' %}
{% set atlantis_loans = 'Atlantis Loans' %}
{% set autoshark = 'AutoShark' %}
{% set bclubbull = 'BclubBull' %}
{% set chainlinkpriceoracle = 'ChainlinkPriceOracle' %}
{% set channels = 'Channels' %}
{% set copyoracle = 'CopyOracle' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set erc1967proxy = 'ERC1967Proxy' %}
{% set el_dorado_exchange = 'El Dorado Exchange' %}
{% set ellipsis = 'Ellipsis' %}
{% set feedregistry = 'FeedRegistry' %}
{% set gambit = 'Gambit' %}
{% set ktx_finance = 'KTX.Finance' %}
{% set keeperregistry = 'KeeperRegistry' %}
{% set level_finance = 'Level Finance' %}
{% set linear_finance = 'Linear Finance' %}
{% set oikos = 'Oikos' %}
{% set optimizedtransparentupgradeableproxy = 'OptimizedTransparentUpgradeableProxy' %}
{% set pancake_bunny = 'Pancake Bunny' %}
{% set pancakepredictionv2 = 'PancakePredictionV2' %}
{% set pancakepredictionv3 = 'PancakePredictionV3' %}
{% set pandorium = 'Pandorium' %}
{% set pricefeeds = 'PriceFeeds' %}
{% set qubit_finance = 'Qubit Finance' %}
{% set radiant = 'Radiant' %}
{% set safuugo = 'SafuuGO' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set uniswapanchoredview = 'UniswapAnchoredView' %}
{% set unverified = 'Unverified' %}
{% set vrfcoordinatorv2 = 'VRFCoordinatorV2' %}
{% set valas_finance = 'Valas Finance' %}
{% set vaultpricefeed = 'VaultPriceFeed' %}
{% set venus = 'Venus' %}
{% set venuschainlinkoracle = 'VenusChainlinkOracle' %}
{% set woo_network = 'WOO Network' %}
{% set wing_finance = 'Wing Finance' %}
{% set wooraclev2 = 'WooracleV2' %}
{% set wooraclev2_1 = 'WooracleV2_1' %}
{% set bzx = 'bZx' %}
{% set dforce = 'dForce' %}
{% set eddiez_bnb = 'eddiez.bnb' %}

SELECT
   'bnb' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{adminupgradeabilityproxy}}', 0x0377954c16c5c47322f3ba09e6c32ef25b62e57b),
  ('{{alpaca_finance}}', 0x634902128543b25265da350e2d961c7ff540fc71),
  ('{{alpaca_finance}}', 0x9fd00faef95cc028bc343bac1fc11e870635b974),
  ('{{annex_finance}}', 0x966ff099d7bddad5bab8e1f93bee642a907639cc),
  ('{{aperocket}}', 0x5d6086f8aae9daebac5674e8f3b867d5743171d3),
  ('{{apollox}}', 0x1b6f2d3844c6ae7d56ceb3c3643b9060ba28feb0),
  ('{{atlantis_loans}}', 0xc9b6d028c0beec6e50cedbf18f2f28976da402d0),
  ('{{autoshark}}', 0x41b471f347a7c2c8e6cb7f4f59c570c6d9c69a3c),
  ('{{bclubbull}}', 0x9a63254329e4265a0f1c54acd690a117bca28394),
  ('{{chainlinkpriceoracle}}', 0x7c37bf8dbd4ae90cdf45d382ceb1580c5d9300cc),
  ('{{channels}}', 0x59b17dd4b570d91ebde62526a08933e0158c25b8),
  ('{{copyoracle}}', 0xa789f9500da9e75d74df0e40c190a6954c1b5cc2),
  ('{{eacaggregatorproxy}}', 0x0567f2323251f0aab15c8dfb1967e4e8a7d42aee),
  ('{{eacaggregatorproxy}}', 0xb97ad0e74fa7d920791e90258a6e2085088b4320),
  ('{{eacaggregatorproxy}}', 0x264990fbd0a4796a3e3d8e37c4d5f87a3aca5ebf),
  ('{{eacaggregatorproxy}}', 0x9ef1b8c0e4f7dc8bf5719ea496883dc6401d5b2e),
  ('{{eacaggregatorproxy}}', 0xcbb98864ef56e9042e7d2efef76141f15731b82f),
  ('{{eacaggregatorproxy}}', 0xb6064ed41d4f67e353768aa239ca86f4f73665a1),
  ('{{eacaggregatorproxy}}', 0x51597f405303c4377e36123cbc172b13269ea163),
  ('{{eacaggregatorproxy}}', 0xa767f745331d267c7751297d982b050c93985627),
  ('{{eacaggregatorproxy}}', 0xc333eb0086309a16aa7c8308dfd32c8bba0a2592),
  ('{{eacaggregatorproxy}}', 0xbf63f430a79d4036a5900c19818aff1fa710f206),
  ('{{eacaggregatorproxy}}', 0xb38722f6a608646a538e882ee9972d15c86fc597),
  ('{{eacaggregatorproxy}}', 0xca236e327f629f9fc2c30a4e95775ebf0b89fac8),
  ('{{eacaggregatorproxy}}', 0x132d3c0b1d2cea0bc552588063bdbb210fdeecfa),
  ('{{eacaggregatorproxy}}', 0x93a67d414896a280bf8ffb3b389fe3686e014fda),
  ('{{eacaggregatorproxy}}', 0x3ab0a0d137d4f946fbb19eecc6e92e64660231c8),
  ('{{eacaggregatorproxy}}', 0x7ca57b0ca6367191c94c8914d7df09a57655905f),
  ('{{eacaggregatorproxy}}', 0xe188a9875af525d25334d75f3327863b2b8cd0f1),
  ('{{eacaggregatorproxy}}', 0xf4c5e535756d11994fcbb12ba8add0192d9b88be),
  ('{{eacaggregatorproxy}}', 0x74e72f37a8c415c8f1a98ed42e78ff997435791d),
  ('{{eacaggregatorproxy}}', 0xd276fcf34d54a926773c399ebaa772c12ec394ac),
  ('{{eacaggregatorproxy}}', 0xe5dbfd9003bff9df5feb2f4f445ca00fb121fb83),
  ('{{eacaggregatorproxy}}', 0x86896feb19d8a607c3b11f2af50a0f239bd71cd0),
  ('{{eacaggregatorproxy}}', 0x0bf79f617988c472dca68ff41efe1338955b9a80),
  ('{{eacaggregatorproxy}}', 0xd7eaa5bf3013a96e3d515c055dbd98dbdc8c620d),
  ('{{eacaggregatorproxy}}', 0xb57f259e7c24e56a1da00f66b55a5640d9f9e7e4),
  ('{{eacaggregatorproxy}}', 0xa8357bf572460fc40f4b0acacbb2a6a61c89f475),
  ('{{eacaggregatorproxy}}', 0x817326922c909b16944817c207562b25c4df16ad),
  ('{{eacaggregatorproxy}}', 0x22db8397a6e77e41471de256a7803829fdc8bc57),
  ('{{eacaggregatorproxy}}', 0x43d80f616daf0b0b42a928eed32147dc59027d41),
  ('{{eacaggregatorproxy}}', 0xd660db62ac9dfafdb401f24268eb285120eb11ed),
  ('{{eacaggregatorproxy}}', 0xf6ef201ae5d05a5cd04d71ab3c90c901d4489e88),
  ('{{eacaggregatorproxy}}', 0x87ea38c9f24264ec1fff41b04ec94a97caf99941),
  ('{{eacaggregatorproxy}}', 0xa3334a9762090e827413a7495afece76f41dfc06),
  ('{{eacaggregatorproxy}}', 0x84210d9013a30c6ab169e28840a6cc54b60fa042),
  ('{{eacaggregatorproxy}}', 0xb056b7c804297279a9a673289264c17e6dc6055d),
  ('{{eacaggregatorproxy}}', 0x38393201952f2764e04b290af9df427217d56b41),
  ('{{eacaggregatorproxy}}', 0x0e8a53dd9c13589df6382f13da6b3ec8f919b323),
  ('{{eacaggregatorproxy}}', 0xcbf8518f8727b8582b22837403cdabc53463d462),
  ('{{eacaggregatorproxy}}', 0x02bfe714e78e2ad1bb1c2bee93ec8dc5423b66d4),
  ('{{eacaggregatorproxy}}', 0x2a3796273d47c4ed363b361d3aefb7f7e2a13782),
  ('{{eacaggregatorproxy}}', 0x116eeb23384451c78ed366d4f67d5ad44ee771a0),
  ('{{erc1967proxy}}', 0x929117a815c5a12e5e1bd17ab24ba19a82d72c54),
  ('{{el_dorado_exchange}}', 0x7391508c76a677517d879d4057910fb66db6f6b2),
  ('{{el_dorado_exchange}}', 0x0a149bfe9f43e6ac49791ae4d8ea2733e75e29f3),
  ('{{ellipsis}}', 0xcce949de564fe60e7f96c85e55177f8b9e4cf61b),
  ('{{feedregistry}}', 0x55328a2df78c5e379a3fee693f47e6d4279c2193),
  ('{{gambit}}', 0x3ddae746330d237b8fc5fa7459b642b1cdfad276),
  ('{{gambit}}', 0x82b1fa2741a6591d30e61830b1cfda0e7ba3abd3),
  ('{{ktx_finance}}', 0xf89b097394c58ad63c8ba2bb46db30a48b4235c7),
  ('{{keeperregistry}}', 0x7b3ec232b08bd7b4b3305be0c044d907b2df960b),
  ('{{level_finance}}', 0x04db83667f5d59ff61fa6bbbd894824b233b3693),
  ('{{level_finance}}', 0x347a868537c96650608b0c38a40d65fa8668bb61),
  ('{{linear_finance}}', 0x475aa5fcdf2eaeaece4f6e83121324cb293911ab),
  ('{{oikos}}', 0xe1ff83762f2db7274b6ac2c1c9bb75b2a8574eaf),
  ('{{optimizedtransparentupgradeableproxy}}', 0x1b2103441a0a108dad8848d8f5d790e4d402921f),
  ('{{pancake_bunny}}', 0xf5bf8a9249e3cc4cb684e3f23db9669323d4fb7d),
  ('{{pancake_bunny}}', 0x542c06a5dc3f27e0fbdc9fb7bc6748f26d54ddb0),
  ('{{pancakepredictionv2}}', 0x18b2a687610328590bc8f2e5fedde3b582a49cda),
  ('{{pancakepredictionv3}}', 0x0e3a8078edd2021dadcde733c6b4a86e51ee8f07),
  ('{{pandorium}}', 0x13768d62457293a1cfd7e34bf69502c2119a511b),
  ('{{pricefeeds}}', 0xb8ac7fabff0d901878c269330b32cdd8d2ba3b8c),
  ('{{qubit_finance}}', 0x20e5e35ba29dc3b540a1aee781d0814d5c77bce6),
  ('{{radiant}}', 0x0bb5c1bc173b207cbf47cdf013617087776f3782),
  ('{{radiant}}', 0xfba3335f443c1351aa7173f23767b6a9c94ac855),
  ('{{safuugo}}', 0x9321bc6185adc9b9cb503cc211e17cb311c3fa95),
  ('{{transparentupgradeableproxy}}', 0x5bae8d7bd3e7e42aa214c44bb3fc6c14ce4f7e49),
  ('{{transparentupgradeableproxy}}', 0xfb25bdaec438cf477dffa6e82e23dd6ea43900b6),
  ('{{uniswapanchoredview}}', 0xc8f2248ab97b42574f86ce4ff7fe054b23795b63),
  ('{{unverified}}', 0x5363bfa2e1ce3c216e474e8291fd500b87c9ec38),
  ('{{unverified}}', 0x1756d409053ec9824934fdbcb9a685ccd545363d),
  ('{{unverified}}', 0xea6c53c9f57307ec4ed274862bcba1a0a7ed435f),
  ('{{unverified}}', 0x733415891849dc689865f8769c2ffdccb8eafc70),
  ('{{vrfcoordinatorv2}}', 0xc587d9053cd1118f25f645f9e08bb98c9712a4ee),
  ('{{valas_finance}}', 0x3436c4b4a27b793539844090e271591cbcb0303c),
  ('{{vaultpricefeed}}', 0x0144b19d1b9338fc7c286d6767bd9b29f0347f27),
  ('{{vaultpricefeed}}', 0xc4ccb5367218e4a84b9cfe989f3c596ad2099ec0),
  ('{{vaultpricefeed}}', 0x076bde59d4588497b602d8916cdb19be34d6be9b),
  ('{{venus}}', 0xd8b6da2bfec71d684d3e2a2fc9492ddad5c3787f),
  ('{{venuschainlinkoracle}}', 0x7fabdd617200c9cb4dcf3dd2c41273e60552068a),
  ('{{woo_network}}', 0x910723e3c6a68276687b50613a1a9e42cc6589b4),
  ('{{wing_finance}}', 0xb51701488e07b5afcc8e1ec046292baa2a4e2770),
  ('{{wooraclev2}}', 0x4b11b9bfaafa840c436a1dddc13d3738c8ebfd62),
  ('{{wooraclev2}}', 0x747f99d619d5612399010ec5706f13e3345c4a9e),
  ('{{wooraclev2_1}}', 0x72dc7fa5eeb901a34173c874a7333c8d1b34bca9),
  ('{{bzx}}', 0x43ccac29802332e1fd3a41264ddbe34ce3073a88),
  ('{{dforce}}', 0x7dc17576200590c4d0d8d46843c41f324da2046c),
  ('{{eddiez_bnb}}', 0x9a45d1e9000ca0b3aa9b5f0c111aca87e65c1c0a),
  ('{{eddiez_bnb}}', 0xbc3a13d4720d41857f5e879a77e89ea60de5aac3)
) a (requester_name, requester_address)
