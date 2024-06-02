{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'optimism' as blockchain,
   node_address,
   operator_name
FROM (values
  (0x71b1d50A1203497492775dc126c3A6e582cE311E, 'cryptomanufaktur'),
  (0x229dAb0e38b6d8858fc1aa904EDf61a4b5C686cF, 'dextrac'),
  (0xB2b315D8eB297Aa599179aFEa34E9BC32A3BAAa5, 'everstake'),
  (0xF06060C6837805D58C2871eB8A113A2EED8BD4E1, 'fiews'),
  (0x962Bb12D7C569C58B64f0d0Dea774366fde95f6F, 'kytzu'),
  (0x5ED81B2f3a1121A803548eEc170E5e8A87C24231, 'linkforest'),
  (0xCA2245C646D272d2D82Eb056876Ae18D63882ce3, 'linkpool'),
  (0x8ccc95646059a639470627f73c7E471B0EE1A6f4, 'linkriver'),
  (0x5EfA53Ed9A84284DE4e2FF08e889957D9d047100, 'newroad'),
  (0x2d44A5AC97d2b0d9433739e1A2B58b2acbA903D5, 'northwestnodes'),
  (0x80Ea642AB8d9536f1112c1703969C95F3E28Ef25, 'omniscience'),
  (0x9B7c9b61bca887b2F008224B0819d50422AE63Ac, 'p2porg'),
  (0x2F69469e34e2E281269E4B070e7ed672748cccDe, 'piertwo'),
  (0x2bD71Dc33AaA57002253f071F84d25402358B74b, 'prophet'),
  (0xfcc955232D26348385237A0bA2B5428920b0aBeC, 'syncnode'),
  (0x90f27A4eDd5e4D486ea63C06019cf8F5D091f507, 'validationcloud'),
  (0xeDCECED1664E53A75F6F864Ed95f88aF45B2276E, 'wetez')
) a (node_address, operator_name)
