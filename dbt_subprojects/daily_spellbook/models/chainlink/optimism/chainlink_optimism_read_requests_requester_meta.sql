{{
  config(
    
    alias='read_requests_requester_meta',
    materialized = 'view'
  )
}}

{% set aave = 'Aave' %}
{% set chainlinkpricefeed = 'ChainlinkPriceFeed' %}
{% set chainlinkpricefeedv2 = 'ChainlinkPriceFeedV2' %}
{% set chainlinkpricefeedv3 = 'ChainlinkPriceFeedV3' %}
{% set eacaggregatorproxy = 'EACAggregatorProxy' %}
{% set pikapricefeed = 'PikaPriceFeed' %}
{% set synthetix = 'Synthetix' %}
{% set transparentupgradeableproxy = 'TransparentUpgradeableProxy' %}
{% set unverified = 'Unverified' %}
{% set vaultpricefeed = 'VaultPriceFeed' %}
{% set wooraclev2 = 'WooracleV2' %}
{% set wooraclev2_1_zipinherit = 'WooracleV2_1_ZipInherit' %}

SELECT
   'optimism' AS blockchain,
   requester_name,
   requester_address
FROM (VALUES
  ('{{aave}}', 0xd81eb3728a631871a7ebbad631b5f424909f0c77),
  ('{{aave}}', 0x9aeefef549323511e027d70562f0c7edcdeb294c),
  ('{{chainlinkpricefeed}}', 0xa36faf16f31c12285467b1973ee8fa144ed4d846),
  ('{{chainlinkpricefeed}}', 0xba073023b8278cbc90bb27490b8279369bf7053b),
  ('{{chainlinkpricefeed}}', 0xc35896651ecd5c71c8cb2d8e88c8f2791b7ec46b),
  ('{{chainlinkpricefeed}}', 0x1b6d49be8a5b2e21ba0dabf0a0ee9223c23a6e65),
  ('{{chainlinkpricefeed}}', 0x0593db04e076817c045287fa56123f6df586190f),
  ('{{chainlinkpricefeed}}', 0x8774a1038471ff73e10b1dbe165287c03289eb8b),
  ('{{chainlinkpricefeed}}', 0xb6dd13b843b1c272452c1bd5f74899544279b4e8),
  ('{{chainlinkpricefeed}}', 0xab3c0f3840dce66846769413f0aec4dbcb06373d),
  ('{{chainlinkpricefeed}}', 0xe5fc30452f61cfbe8914e0bcaa752cd10cbf3949),
  ('{{chainlinkpricefeed}}', 0xf70c85165c1728f4780ef5caf66705c382c32c6e),
  ('{{chainlinkpricefeed}}', 0xf44e50784264d4f227fbf426484a9645704367b2),
  ('{{chainlinkpricefeed}}', 0xcad994415747355c8a2e6556d1021905b4e204ce),
  ('{{chainlinkpricefeedv2}}', 0x94740ca2c47e13b7b0d3f44fc552ecc880e1d824),
  ('{{chainlinkpricefeedv2}}', 0x7d462952c003b80fe16bbe826e4ae34cfc4aebb9),
  ('{{chainlinkpricefeedv2}}', 0xbe7dc0896f0f0580640266ee5228942e15561331),
  ('{{chainlinkpricefeedv2}}', 0x48b6d0b608f330126a04bebc17f976fcfb3a3fd3),
  ('{{chainlinkpricefeedv2}}', 0x8f9acef0cdc067819cf3ce7fbe23148cb1bf851e),
  ('{{chainlinkpricefeedv2}}', 0x19a4dea5470b2d8e16d9f8d929c0b36fe44113b4),
  ('{{chainlinkpricefeedv2}}', 0x1942eba8d0f49d7e9687b40702c4e08a2abab541),
  ('{{chainlinkpricefeedv2}}', 0x1635c48cbe5d5a927ef2d0283cc0895a9b6e997a),
  ('{{chainlinkpricefeedv2}}', 0x0f0890e41daff66b518bb25dcd98b37acc839874),
  ('{{chainlinkpricefeedv2}}', 0xbbfa0478ad6e5d5040cd21f7aca64e56ff3426e6),
  ('{{chainlinkpricefeedv2}}', 0xc6cdc0414cce84021bbf077ec4e35735b1e1222c),
  ('{{chainlinkpricefeedv2}}', 0x99eb05388e5afb15795f63921195094999155971),
  ('{{chainlinkpricefeedv2}}', 0xfda08aef411a83140f6ac5fa516ff37482d5e5cb),
  ('{{chainlinkpricefeedv2}}', 0x016becff054ffd6f16727e6070f4a5a1127053a1),
  ('{{chainlinkpricefeedv2}}', 0x95b00f33d83173059c9ee9c108f9b78d8d35c7e7),
  ('{{chainlinkpricefeedv2}}', 0x3b3bfe1d3f986dbf34feb25256fef544b0fc46ba),
  ('{{chainlinkpricefeedv2}}', 0x3b0f28ba2c0743d968cc356cf277560108cb3f37),
  ('{{chainlinkpricefeedv2}}', 0x5e359becff1c8f6a1d5dbeb5f115dbdb1561ea41),
  ('{{chainlinkpricefeedv2}}', 0x0fe5ee407534611881ec6d7957c9ead11a851195),
  ('{{chainlinkpricefeedv3}}', 0x7913c7da57a53b58ab98b7a569d3f2f23caec981),
  ('{{chainlinkpricefeedv3}}', 0x4b1153fca71bc33d911b3bf01b0003afe6b279e2),
  ('{{chainlinkpricefeedv3}}', 0x60417ec91411c884cc69408a6920975953482ab2),
  ('{{chainlinkpricefeedv3}}', 0xa84fd728a7cd878d0aec4ce155677d720a3185a8),
  ('{{chainlinkpricefeedv3}}', 0x6519b73b495958e989fc2f26b529d0741e8ac8e8),
  ('{{chainlinkpricefeedv3}}', 0x5ce0616eae872fbb13cc15488583967ef8750e45),
  ('{{chainlinkpricefeedv3}}', 0x08b719cdc57d7e07f4dab7a6e97bba3bc90fc326),
  ('{{chainlinkpricefeedv3}}', 0x959c99e14af1e9312d176eb6828ac890e6ef18c1),
  ('{{chainlinkpricefeedv3}}', 0x62f5a47dde7a2de8c5dbe4d415b27a708564f8c1),
  ('{{chainlinkpricefeedv3}}', 0xebbd6c12c341380c073f5299cebaa673ddd0190e),
  ('{{chainlinkpricefeedv3}}', 0x3d9a8e57e4e1b3ea511509e8f67d0c25fdcabd0b),
  ('{{chainlinkpricefeedv3}}', 0x634684e3f0187cc45c427090227e85ef580eba6f),
  ('{{chainlinkpricefeedv3}}', 0x849383de33378e7dc0fd05f5d03daa33f231dc90),
  ('{{chainlinkpricefeedv3}}', 0x78ab37e32c04efa43cbfb782dad1af3a919c597a),
  ('{{chainlinkpricefeedv3}}', 0x1734436f851cc3e3384c55527b692b93da19cc72),
  ('{{chainlinkpricefeedv3}}', 0x8f1b5ed853bda4f5bd164373de22e5240aa83494),
  ('{{chainlinkpricefeedv3}}', 0xe82db4e747ac9245ee6020cdabe92a9eed4d949e),
  ('{{chainlinkpricefeedv3}}', 0xe1cfba047ef7f009c79dd9dabd8c464938a4fa90),
  ('{{chainlinkpricefeedv3}}', 0xc31bf706e2309de50727ec86ee2b607e7869fa5a),
  ('{{chainlinkpricefeedv3}}', 0x972dbdeee88898a59ff88ae9a4cff3b6d61961d9),
  ('{{chainlinkpricefeedv3}}', 0x156e878c09e480e86d8c72a8484fea1d2066e5d4),
  ('{{chainlinkpricefeedv3}}', 0xd707799ceac0ac103eedd3ca6bde9ad6fa8bf091),
  ('{{chainlinkpricefeedv3}}', 0x2d0e51c782d609854177c6a61aacb25f329a2f16),
  ('{{chainlinkpricefeedv3}}', 0x47334de966aa36bdbaeb480a86050a9b532e6599),
  ('{{chainlinkpricefeedv3}}', 0x37b5c40b14e0b81adba533507c43946c09b34252),
  ('{{chainlinkpricefeedv3}}', 0x8f38fc5a38be39d8c477b9d9076e9504f23e7933),
  ('{{chainlinkpricefeedv3}}', 0x38b2535b4d6d395d505aeac63200ad8783a08a3b),
  ('{{chainlinkpricefeedv3}}', 0x4b9ff76ae449a97f551875eabb2997bd70b8ee19),
  ('{{chainlinkpricefeedv3}}', 0x6c243db5f0470fcf692e481ec656cfad5a825d9c),
  ('{{eacaggregatorproxy}}', 0x13e3ee699d1909e989722e753853ae30b17e08c5),
  ('{{eacaggregatorproxy}}', 0x0d276fc14719f9292d5c1ea2198673d1f4269246),
  ('{{eacaggregatorproxy}}', 0x16a9fa2fda030272ce99b29cf780dfa30361e0f3),
  ('{{eacaggregatorproxy}}', 0xd702dd976fb76fffc2d3963d037dfdae5b04e593),
  ('{{eacaggregatorproxy}}', 0x338ed6787f463394d24813b297401b9f05a8c9d1),
  ('{{eacaggregatorproxy}}', 0xc663315f7af904fbbb0f785c32046dfa03e85270),
  ('{{eacaggregatorproxy}}', 0x89178957e9bd07934d7792ffc0cf39f11c8c2b1f),
  ('{{eacaggregatorproxy}}', 0xcc232dcfaae6354ce191bd574108c1ad03f86450),
  ('{{eacaggregatorproxy}}', 0xca6fa4b8cb365c02cd3ba70544efffe78f63ac82),
  ('{{eacaggregatorproxy}}', 0xc19d58652d6bfc6db6fb3691eda6aa7f3379e4e9),
  ('{{eacaggregatorproxy}}', 0xef89db2ea46b4ad4e333466b6a486b809e613f39),
  ('{{eacaggregatorproxy}}', 0x0ded608afc23724f614b76955bbd9dfe7dddc828),
  ('{{eacaggregatorproxy}}', 0xa12cddd8e986af9288ab31e58c60e65f2987fb13),
  ('{{eacaggregatorproxy}}', 0x2ff1eb7d0cec35959f0248e9354c3248c6683d9b),
  ('{{eacaggregatorproxy}}', 0x5087dc69fd3907a016bd42b38022f7f024140727),
  ('{{eacaggregatorproxy}}', 0xd38579f7cbd14c22cf1997575ea8ef7bfe62ca2c),
  ('{{eacaggregatorproxy}}', 0xae33e077a02071e62d342e449afd9895b016d541),
  ('{{eacaggregatorproxy}}', 0xbd92c6c284271c227a1e0bf1786f468b539f51d9),
  ('{{eacaggregatorproxy}}', 0xc6066533917f034cf610c08e1fe5e9c7eade0f54),
  ('{{eacaggregatorproxy}}', 0x7cfb4fac1a2fdb1267f8bc17fadc12804ac13cfe),
  ('{{eacaggregatorproxy}}', 0x94a178f2c480d14f8cdda908d173d7a73f779cb7),
  ('{{eacaggregatorproxy}}', 0x2fcf37343e916eaed1f1ddaaf84458a359b53877),
  ('{{eacaggregatorproxy}}', 0x8dba75e83da73cc766a7e5a0ee71f656bab470d6),
  ('{{eacaggregatorproxy}}', 0x37aafb2ee35f1250a001202c660b13c301d2130b),
  ('{{eacaggregatorproxy}}', 0x4e1c6b168dcfd7758bc2ab9d2865f1895813d236),
  ('{{pikapricefeed}}', 0xdb4174e1a4005a30f5a0924f43c8dfcb8cbd828a),
  ('{{synthetix}}', 0x913bd76f7e1572cc8278cef2d6b06e2140ca9ce2),
  ('{{synthetix}}', 0x59b01789bf268c7c77451d02758621990bb50bbf),
  ('{{synthetix}}', 0xeb66fc1bfdf3284cb0ca1de57149dcf3cefa5453),
  ('{{synthetix}}', 0x22602469d704bffb0936c7a7cfcd18f7aa269375),
  ('{{synthetix}}', 0x0ca3985f973f044978d2381afed9c4d85a762d11),
  ('{{transparentupgradeableproxy}}', 0xbea170c49036d7917cda7511b57cafd41efedc01),
  ('{{transparentupgradeableproxy}}', 0xf4aef21d906992afadde7a9676e1db4feb6390dd),
  ('{{unverified}}', 0x8d826ae53d0d7acfa97db71c00cb26aa4ce44e52),
  ('{{vaultpricefeed}}', 0xc37ad9d78fb001a2429b2401e91c2dd849595798),
  ('{{vaultpricefeed}}', 0x6a9d1d277acc62cf8f3293263cbee9b5832b3844),
  ('{{wooraclev2}}', 0x464959ad46e64046b891f562cff202a465d522f3),
  ('{{wooraclev2_1_zipinherit}}', 0xd589484d3a27b7ce5c2c7f829eb2e1d163f95817)
) a (requester_name, requester_address)
