{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'ethereum' as blockchain,
   node_address,
   operator_name
FROM (values
  (0xfc038715c79Ebcf7F9ee5723E466454B21434157, 'cryptomanufaktur'),
  (0x96d1D86b1BEd64053410FdCc2E3585EB578DdE1f, 'dextrac'),
  (0xd086b29d92C8D15d187e8c65B79Ba0C44C8326c2, 'everstake'),
  (0xE336C8e4B6649c82A16a7c78577169A24Baa7fff, 'fiews'),
  (0xA39B7c0f08e4727c8325b4ad043513AA5185a4E2, 'kytzu'),
  (0xa968cf59aB2BaE618f6eE0a80EcBd5b242ebE991, 'linkforest'),
  (0x90f91a0fFDC93a11c045b3155F0b3cc0D9fB9ef6, 'linkpool'),
  (0x465Cb88B0Bf2A984a7C6c053262C8137D667bEaE, 'linkriver'),
  (0xc333b76845bDF806369EF0F00134559988aa985C, 'newroad'),
  (0x31eD28c2549e0195c4A405B71e4f18EfB935bE6f, 'northwestnodes'),
  (0xa616AEEa440ECfb1AA8065a19E6E55652743B3FB, 'omniscience'),
  (0x316D2E43270ff4091Ca5d269c0E5cD8363524C91, 'p2porg'),
  (0xf547696fF576aeA0D2C8e41D467daD4CeE904513, 'piertwo'),
  (0xCEED45aD0f1c8E621eef28a4643B06AF04A6dEB0, 'prophet'),
  (0xd7d7f77069aCEF3116B6D0eDBEA48e45aCc3562e, 'syncnode'),
  (0x6A985273Db73f21D6a74Ee9f76725112819BD950, 'validationcloud'),
  (0xFc52B2196a94D08fc9614b8039821bcE03bF58E8, 'wetez')
) a (node_address, operator_name)
