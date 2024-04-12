{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'base' as blockchain,
   node_address,
   operator_name
FROM (values
  (0x2b06E81cd29Ec579Ca61B7205B7C04F61eFE1135, 'cryptomanufaktur'),
  (0x458807fc1e375aA814037D31E9608214a5d89f24, 'dextrac'),
  (0x49Fc8CbD8A6Cff529eF7C498c812f6f3f33f005E, 'everstake'),
  (0xb3D4D386B89b5D04a3dB43B39Ec24A95751e4Cf9, 'fiews'),
  (0x409E2F15d1668aAD747242af6df32D0f9888994e, 'kytzu'),
  (0x31FB00b1F2De8106b087cf589Fa9e54ffF2c538D, 'linkforest'),
  (0xA7f25cbCb678b61D72aF87867bf85677260b2b68, 'linkpool'),
  (0x0b9beBA52349c567dF5c3793fFb061D851d37901, 'linkriver'),
  (0x186363157bFAc7e301C56a1Ab9BAFBaa88713d05, 'newroad'),
  (0xC4163681c352D832c32F930e205ccb63B1dc08Be, 'northwestnodes'),
  (0x8818A0e206E103CaEBFC7a67aB711e637B2Cd3F8, 'omniscience'),
  (0x59446c1A13c5D1182cA50f2080C86a18dF84A4ED, 'p2porg'),
  (0xdaca43Cb6449CB739A63cA1eF0927F63995c0aEE, 'piertwo'),
  (0xBAb7937F6c0f3d87F7Beb7a8E15330603c2AB9E0, 'prophet'),
  (0x6C5Ca2977f3a5E75Bab2e2DE5815ff8c5D6c6775, 'syncnode'),
  (0x8Cb973bdeAcE0Ad71bbB57E4901390caA952267D, 'validationcloud'),
  (0x5a3dF9bD4Cf07510dB5812aC071c2F83895122c2, 'wetez')
) a (node_address, operator_name)
