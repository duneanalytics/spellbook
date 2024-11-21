{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'avalanche_c' as blockchain,
   node_address,
   operator_name
FROM (values
  (0x5b7fFA174f432A4Ad6F3b493ddAf5F5BCA0BaaA1, 'cryptomanufaktur'),
  (0xb8F87E376Ef984d154DA9C63b9D06740369F2B49, 'dextrac'),
  (0x206598DAc5206fc1c26745050eEbD3Ee80F4e6ba, 'everstake'),
  (0xEacB0b11675650ed8Bc48c42eE72e55d480e9F81, 'fiews'),
  (0xD808878Af04941eEE5F4685D68aFBf2130Dfe6f5, 'kytzu'),
  (0x7b9c30FF13cb28710bB6b20c1ac4C7938309AF27, 'linkforest'),
  (0x223e46855d1f9A04C80F2D044512D847307508E8, 'linkpool'),
  (0xB5A91c2adFbcB2DA16DC542Eb5c7c54e4c8D45a5, 'linkriver'),
  (0xeC55779329cBb18B49F748870C8Ad4328f6E7fC8, 'newroad'),
  (0x4499a00546aBa51124dfd7b27A17e7653cF125df, 'northwestnodes'),
  (0x9F36617a44caCc71868aC6FC6C5BE3c9Aa2E3775, 'omniscience'),
  (0x93246EfeffC97238B5dE72F14AbbC29fd5F66b65, 'p2porg'),
  (0xed462A9F2Bc31eA3f3255597091897585cDaA344, 'piertwo'),
  (0x8c6Bb4E3Ce25F723b9C9433904a2b585A68763c8, 'prophet'),
  (0x4384bc89E8342aaFCAa467a9f891E0390f99C430, 'syncnode'),
  (0x7ee01CdbbaA7258C802BBf5e94C233c1884B908b, 'validationcloud'),
  (0x84cEb6f75561dF86dB6e127c286B6efF1e3239B2, 'wetez')
) a (node_address, operator_name)
