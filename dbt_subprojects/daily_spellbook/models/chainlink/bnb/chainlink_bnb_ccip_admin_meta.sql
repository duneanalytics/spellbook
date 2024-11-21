{{
  config(
    alias='ccip_admin_meta',
	materialized = 'view'
  )
}}

SELECT
   'bnb' as blockchain,
   admin_address,
   operator_name
FROM (values
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, 'cryptomanufaktur'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, 'dextrac'),
  (0x7AF3C2b54eE2f170b8104222eB4EDf2511f5d9d0, 'everstake'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, 'fiews'),
  (0x001E0d294383d5b4136476648aCc8D04a6461Ae3, 'kytzu'),
  (0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e, 'linkforest'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, 'linkpool'),
  (0x14f94049397C3F1807c45B6f854Cb5F36bC4393B, 'linkriver'),
  (0x670dBCf722ee66079DAB6456071e8d536eEC1463, 'newroad'),
  (0x0921E157b690c4F89F7C2a210cFd8bF3964F6776, 'northwestnodes'),
  (0x6D837Ef25d10d26BDc8629E3390F2e7ff6261D99, 'omniscience'),
  (0xCDa423ee5A7A886eF113b181469581306fC8B607, 'p2porg'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, 'piertwo'),
  (0xBDB624CD1051F687f116bB0c642330B2aBdfcc06, 'prophet'),
  (0xC51D3470693BC049809A1c515606124c7C75908d, 'syncnode'),
  (0x183A96629fF566e7AA8AfA38980Cd037EB40A59A, 'validationcloud'),
  (0x111f1B41f702c20707686769a4b7f25c56C533B2, 'wetez')
) a (admin_address, operator_name)
