{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_meta'),
    materialized = 'view'
  )
}}

{% set aave = 'AAVE' %}
{% set aave = 'Aave' %}
{% set adminupgradeabilityproxy = 'AdminUpgradeabilityProxy' %}
{% set aggregatorfacade = 'AggregatorFacade' %}
{% set chainlink = 'Chainlink' %}
{% set chainlinkethpriceoracleproxy = 'ChainlinkETHPriceOracleProxy' %}
{% set chainlinklinkusdcpriceoracleproxy = 'ChainlinkLINKUSDCPriceOracleProxy' %}
{% set chainlinksnxusdcpriceoracleproxy = 'ChainlinkSNXUSDCPriceOracleProxy' %}
{% set chainlinkwbtcusdcpriceoracleproxy = 'ChainlinkWBTCUSDCPriceOracleProxy' %}
{% set chainlinkyfiusdcpriceoracleproxy = 'ChainlinkYFIUSDCPriceOracleProxy' %}
{% set clipperpool = 'ClipperPool' %}
{% set compound = 'Compound' %}
{% set cream_finance = 'Cream.Finance' %}
{% set dash_2_trade = 'Dash 2 Trade' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set ens = 'ENS' %}
{% set feedregistry = 'FeedRegistry' %}
{% set impt = 'Impt' %}
{% set inverse_finance = 'Inverse Finance' %}
{% set liquity = 'Liquity' %}
{% set mcdex = 'MCDEX' %}
{% set nxmdsvalue = 'NXMDSValue' %}
{% set nexus_mutual = 'Nexus Mutual' %}
{% set paraspaceoracle = 'ParaSpaceOracle' %}
{% set priceoracleproxy = 'PriceOracleProxy' %}
{% set rari_capital = 'Rari Capital' %}
{% set sale = 'Sale' %}
{% set swarm_markets = 'Swarm Markets' %}
{% set synthetix = 'Synthetix' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set unfederalreserve = 'UnFederalReserve' %}
{% set vrfcoordinatorv2 = 'VRFCoordinatorV2' %}
{% set valueinterpreter = 'ValueInterpreter' %}
{% set wall_street_memes = 'Wall Street Memes' %}
{% set bzx_vesting_token = 'bZx Vesting Token' %}
{% set cryptomaniacs_eth = 'cryptomaniacs.eth' %}

SELECT
   'ethereum' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{aave}}', 0x9b26214bec078e68a394aaebfbfff406ce14893f),
  ('{{aave}}', 0xa50ba011c48153de246e5192c8f9258a2ba79ca9),
  ('{{aave}}', 0x76b47460d7f7c5222cfb6b6a75615ab10895dde4),
  ('{{aave}}', 0x54586be62e3c3580375ae3723c145253060ca0c2),
  ('{{aave}}', 0xac4a2ac76d639e10f2c05a41274c1af85b772598),
  ('{{adminupgradeabilityproxy}}', 0x05b1d5b3ad20769b3b71b658a1df2290cd5a2376),
  ('{{aggregatorfacade}}', 0xb103ede8acd6f0c106b7a5772e9d24e34f5ebc2c),
  ('{{aggregatorfacade}}', 0x85ab3512465f39b8bb40a8872f8fbfd5f08ace1e),
  ('{{chainlink}}', 0xdc3ea94cd0ac27d9a86c180091e7f78c683d3699),
  ('{{chainlink}}', 0x7c5d4f8345e66f68099581db340cd65b078c41f4),
  ('{{chainlink}}', 0xd6aa3d25116d8da79ea0246c4826eb951872e02e),
  ('{{chainlink}}', 0x8a12be339b0cd1829b91adc01977caa5e9ac121e),
  ('{{chainlink}}', 0xa8e875f94138b0c5b51d1e1d5de35bbddd28ea87),
  ('{{chainlink}}', 0x14e613ac84a31f709eadbdf89c6cc390fdc9540a),
  ('{{chainlink}}', 0xae48c91df1fe419994ffda27da09d5ac69c30f55),
  ('{{chainlink}}', 0xced2660c6dd1ffd856a5a82c67f3482d88c50b12),
  ('{{chainlink}}', 0xdbd020caef83efd542f4de03e3cf0c28a4428bd5),
  ('{{chainlink}}', 0x6af09df7563c363b5763b9102712ebed3b9e859b),
  ('{{chainlink}}', 0x5239a625deb44bf3eeac2cd5366ba24b8e9db63f),
  ('{{chainlink}}', 0xa9a88f8bdffa157c7a0d6e82e27e5f7164daf8fe),
  ('{{chainlink}}', 0x9f0f69428f923d6c95b781f89e165c9b2df9789d),
  ('{{chainlinkethpriceoracleproxy}}', 0xb503cd2492b871ef3d6d972777814934011bf29c),
  ('{{chainlinklinkusdcpriceoracleproxy}}', 0x809ccf986464bbd278d94fa87b6e832e6d6acbb4),
  ('{{chainlinksnxusdcpriceoracleproxy}}', 0x0a870717a19a4b4cd37a31d58ebf28bccc07d4da),
  ('{{chainlinkwbtcusdcpriceoracleproxy}}', 0xb77ddd925e69ddca799d9cc6abea5ae2c7c7f780),
  ('{{chainlinkyfiusdcpriceoracleproxy}}', 0x3561eafb0a3cbf8e7e0fbd55d6d97daff6a9c987),
  ('{{clipperpool}}', 0xe82906b6b1b04f631d126c974af57a3a7b6a99d9),
  ('{{compound}}', 0xc3d688b66703497daa19211eedff47f25384cdc3),
  ('{{cream_finance}}', 0xde19f5a7cf029275be9cec538e81aa298e297266),
  ('{{cream_finance}}', 0x4b7dba23bea9d1a2d652373bcd1b78b0e9e0188a),
  ('{{cream_finance}}', 0x647a539282e8456a64dfe28923b7999b66091488),
  ('{{dash_2_trade}}', 0x6448d7a20ece8c57212ad52b362b5c9b4feac27d),
  ('{{eacaggregatorproxy}}', 0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419),
  ('{{eacaggregatorproxy}}', 0x986b5e1e1755e3c2440e960477f25201b0a8bbd4),
  ('{{eacaggregatorproxy}}', 0x773616e4d11a78f511299002da57a0a94577f1f4),
  ('{{eacaggregatorproxy}}', 0xee9f2375b4bdf6387aa8265dd4fb8f16512a1d46),
  ('{{eacaggregatorproxy}}', 0xdc530d9457755926550b59e8eccdae7624181557),
  ('{{eacaggregatorproxy}}', 0xf4030086522a5beea4988f8ca5b36dbc97bee88c),
  ('{{eacaggregatorproxy}}', 0xdeb288f737066589598e9214e782fa5a8ed689e8),
  ('{{eacaggregatorproxy}}', 0x8fffffd4afb6115b954bd326cbe7b4ba576818f6),
  ('{{eacaggregatorproxy}}', 0xaed0c38402a5d19df6e4c03f4e2dced6e29c1ee9),
  ('{{eacaggregatorproxy}}', 0x2c1d072e956affc0d435cb7ac38ef18d24d9127c),
  ('{{eacaggregatorproxy}}', 0x3e7d1eab13ad0104d2750b8863b489d65364e32d),
  ('{{eacaggregatorproxy}}', 0x6df09e975c830ecae5bd4ed9d90f3a95a4f88012),
  ('{{eacaggregatorproxy}}', 0x86392dc19c0b719886221c78ab11eb8cf5c52812),
  ('{{eacaggregatorproxy}}', 0x0981af0c002345c9c5ad5efd26242d0cbe5aca99),
  ('{{eacaggregatorproxy}}', 0x169e633a2d1e6c10dd91238ba11c4a708dfef37c),
  ('{{eacaggregatorproxy}}', 0xb49f677943bc038e9857d61e7d053caa2c1734c1),
  ('{{eacaggregatorproxy}}', 0x3886ba987236181d98f2401c507fb8bea7871df2),
  ('{{eacaggregatorproxy}}', 0xe572cef69f43c2e488b33924af04bdace19079cf),
  ('{{eacaggregatorproxy}}', 0x79291a9d692df95334b1a0b3b4ae6bc606782f8c),
  ('{{eacaggregatorproxy}}', 0x614715d2af89e6ec99a233818275142ce88d1cfd),
  ('{{eacaggregatorproxy}}', 0xc7de7f4d4c9c991ff62a07d18b3e31e349833a18),
  ('{{eacaggregatorproxy}}', 0x24551a8fb2a7211a25a17b1481f043a8a8adc7f2),
  ('{{eacaggregatorproxy}}', 0x3147d7203354dc06d9fd350c7a2437bca92387a4),
  ('{{eacaggregatorproxy}}', 0x8e0b7e6062272b5ef4524250bfff8e5bd3497757),
  ('{{eacaggregatorproxy}}', 0xbcf5792575ba3a875d8c406f4e7270f51a902539),
  ('{{eacaggregatorproxy}}', 0xb9e1e3a9feff48998e45fa90847ed4d467e8bcfd),
  ('{{eacaggregatorproxy}}', 0xa027702dbb89fbd58938e4324ac03b58d812b0e1),
  ('{{eacaggregatorproxy}}', 0x82a44d92d6c329826dc557c5e1be6ebec5d5feb9),
  ('{{eacaggregatorproxy}}', 0x24d9ab51950f3d62e9144fdc2f3135daa6ce8d1b),
  ('{{ens}}', 0xcf7fe2e614f568989869f4aade060f4eb8a105be),
  ('{{ens}}', 0x7542565191d074ce84fbfa92cae13acb84788ca9),
  ('{{ens}}', 0x63faf46dadc9676745836289404b39136622b821),
  ('{{ens}}', 0xa51b83e420c5f82982dc8b7f4514c9bea0b290ee),
  ('{{feedregistry}}', 0x47fb2585d2c56fe188d0e6ec628a38b74fceeedf),
  ('{{impt}}', 0xf2e391f11cd1609679d03a1ac965b1d0432a7007),
  ('{{inverse_finance}}', 0xe8929afd47064efd36a7fb51da3f8c5eb40c4cb4),
  ('{{liquity}}', 0x4c517d4e2c851ca76d7ec94b805269df0f2201de),
  ('{{mcdex}}', 0x9b2d7d7f7b2810ef2be979fc2acebe6097d9563a),
  ('{{mcdex}}', 0xcfa46e1b666fd91bf39028055d506c1e4ca5ad6e),
  ('{{mcdex}}', 0x133906776302d10a2005ec2ed0c92ab6f2cbd903),
  ('{{nxmdsvalue}}', 0x17e76d3fcf40fa8acffdffe192bf4e8fb264d9fb),
  ('{{nexus_mutual}}', 0xc628050cc70d761fa62b8af7d1ef4ca883c2fd3b),
  ('{{paraspaceoracle}}', 0x6b58baa08a91f0f08900f43692a9796045454a17),
  ('{{priceoracleproxy}}', 0x940ce2a25b0ba48d213acc13abc21d9fee2ed6dd),
  ('{{rari_capital}}', 0xe102421a85d9c0e71c0ef1870dac658eb43e1493),
  ('{{rari_capital}}', 0xb0602af43ca042550ca9da3c33ba3ac375d20df4),
  ('{{sale}}', 0x826bb51954b93f1972a3472abf6dcd6672adb462),
  ('{{swarm_markets}}', 0xc1b06da65074c9df0109d312eb3e59e04f638514),
  ('{{synthetix}}', 0xe95ef4e7a04d2fb05cb625c62ca58da10112c605),
  ('{{synthetix}}', 0x9d7f70af5df5d5cc79780032d47a34615d1f1d77),
  ('{{synthetix}}', 0xba727c69636491ecdfe3e6f64cbe9428ad371e48),
  ('{{synthetix}}', 0xd69b189020ef614796578afe4d10378c5e7e1138),
  ('{{synthetix}}', 0xbcc4ac49b8f57079df1029dd3146c8ecd805acd0),
  ('{{synthetix}}', 0xdb2ae36c2e9c00070e5bf752be1fa2d477e98bda),
  ('{{synthetix}}', 0xa68c6020ff9ea79f05345cdd2ce37df4b89478ed),
  ('{{synthetix}}', 0xb4dc5ced63c2918c89e491d19bf1c0e92845de7c),
  ('{{synthetix}}', 0x648280dd2db772cd018a0cec72fab5bf8b7683ab),
  ('{{synthetix}}', 0xda80e6024bc82c9fe9e4e6760a9769cf0d231e80),
  ('{{synthetix}}', 0x5810fc0e79f4323b2dd3c638914083fd23a941c0),
  ('{{transparentupgradeableproxy}}', 0xb2d7e4d79056c6c439ced389892feb7b0cf35f97),
  ('{{transparentupgradeableproxy}}', 0x65c2e54a4c75ff6da7b6b32369c1677250075fb2),
  ('{{transparentupgradeableproxy}}', 0xcf6efe6cad0f9428f2b5f95cff90122dcd3bb4bc),
  ('{{unfederalreserve}}', 0x9c48aaee3243a2e0c3b991386dc39f0027cf2ad7),
  ('{{vrfcoordinatorv2}}', 0x271682deb8c4e0901d1a1550ad2e64d568e69909),
  ('{{valueinterpreter}}', 0xd7b0610db501b15bfb9b7ddad8b3869de262a327),
  ('{{wall_street_memes}}', 0xfb071837728455c581f370704b225ac9eabdfa4a),
  ('{{bzx_vesting_token}}', 0x5abc9e082bf6e4f930bbc79742da3f6259c4ad1d),
  ('{{cryptomaniacs_eth}}', 0xbb1d497eda7533e71a96d2be6c38b96cf6611903)
) a (requester_name, requester_address)
