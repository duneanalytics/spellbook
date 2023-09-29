{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set chainlayer = 'Chainlayer' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}

SELECT node_address, operator_name FROM (VALUES
  (0xf9b9fbeB0512283a445F1A948a78AF8f954b3343, '{{a01node}}'),
  (0x2539ffc9DeD82926A5AAEe065E800C7d1DE02454, '{{a01node}}'),
  (0xdD0955D12962044D427753F6C9b6C241d2A7D965, '{{a01node}}'),
  (0xB834eD914228B68b0E1f3E4F1e168Ed2b580a35c, '{{blockdaemon}}'),
  (0xF69D0bBc95db6287ef02F19E5B2789972f776C2F, '{{blockdaemon}}'),
  (0x2878c587eba4C4251f97784cE124f7387305Ab32, '{{chainlayer}}'),
  (0xaE00F7474dCc5A0E9f4D7a65DFb938c275f75dB6, '{{chainlayer}}'),
  (0x2bdF9249c350C68a43a9714c1b9153af54751b1C, '{{cryptomanufaktur}}'),
  (0xFA011d8d6C26F13Abe2CEFEd38226E401B2b8a99, '{{cryptomanufaktur}}'),
  (0x6CA4166EeFd64B5f607f12Fb0C3fbAe233897757, '{{dextrac}}'),
  (0xA6C37c771d81CeB7Dda952cc5625e06444067e2f, '{{dextrac}}'),
  (0x85C8aAA2232D12158C4cA731543470bc9e9bb29D, '{{fiews}}'),
  (0xd44Fd1c1fE3F3F0d93a8867AF4041AB231783FcB, '{{fiews}}'),
  (0x8CFc4459394801D2EA78F9cEeB31a7f58B8CdDDe, '{{inotel}}'),
  (0x57fe8051B897a14b4cbF84c22e7EEEA0b30bA903, '{{inotel}}'),
  (0xa04163e4033a941A3446c33e432A1da7ecc8898A, '{{linkpool}}'),
  (0x70B17C0Fe982aB4A7AC17A4c25485643151A1F2d, '{{linkpool}}'),
  (0xD46acbA18e4f3C8b8b6c501DF1a6B05609a642Bd, '{{linkriver}}'),
  (0x689d0367b9D654Aae886982894896f3A826840ED, '{{linkriver}}'),
  (0x56873b5E7299d2C0d4ab28d9128D734CF3bf8398, '{{piertwo}}'),
  (0xB1f0D485227DbeCFf6F1c0F28a58bBa1a97D4D81, '{{piertwo}}'),
  (0x91722db88a8810e2e4AE2E4549aeE9eb2B9A4e8A, '{{simplyvc}}'),
  (0xE5e7492282FD1E3bfAC337A0BecCD29b15B7B240, '{{simplyvc}}')
) AS tmp_node_meta(node_address, operator_name)
