{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'polygon' as blockchain,
   node_address,
   operator_name
FROM (values
  (0x2a8213afA1C17b56FFD4E74bc9B8a6a501c2Ca59, 'cryptomanufaktur'),
  (0x577071e8cb406BB1643adde495AD04E926BbD35f, 'dextrac'),
  (0x7F797646dF0432f3A5bB89F34ab1f114513dAd84, 'everstake'),
  (0x68Df61A58BEb7f38979DDef92577B641Fc1cF93c, 'fiews'),
  (0x9E604B15222f16B3A009a6cc9101630D09307aE8, 'kytzu'),
  (0xEc1E746DD5F4FadDE8234C4BD6163802f504e000, 'linkforest'),
  (0x0B996B8C217F8fd087B8C1e6231ac99f43Cd2115, 'linkpool'),
  (0x814cdA4aA8A5E2c83dF5eB66970695a9CAA373C9, 'linkriver'),
  (0xf01C2Bc5bf3C9178aB5C1603D1041fb4389B0C7F, 'newroad'),
  (0x9f94408A1d6ad2993671753ad42BaE8b1Add24D5, 'northwestnodes'),
  (0x25AdB1dd419b83235478594903b557cCa528A549, 'omniscience'),
  (0x1E5bb4FfB55252A54294D2093d30f3671fFEE6B3, 'p2porg'),
  (0x5f99e976Da7CE1213E29e38726627F46b270A640, 'piertwo'),
  (0x28D0D41d7c260b5110f9Ad4D4c67712EA0B0EDB3, 'prophet'),
  (0x5bf83a4944b59B8A163D546B3A57f5AaE7938ff1, 'syncnode'),
  (0xB8395157BB560386148274540Df48D12cc5F9B78, 'validationcloud'),
  (0xB4c03485172079e82A6ed7DE3535d7d12E5e8D8F, 'wetez')
) a (node_address, operator_name)
