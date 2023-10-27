{{
  config(
    
    alias='automation_meta',
    materialized = 'view'
  )
}}

{% set _01node = '01node' %}
{% set alphachain = 'alphachain' %}
{% set anyblock = 'anyblock' %}
{% set chainlayer = 'chainlayer' %}
{% set cryptomanufaktur = 'cryptomanufaktur' %}
{% set dextrac = 'dextrac' %}
{% set fiews = 'fiews' %}
{% set inotel = 'inotel' %}
{% set linkforest = 'linkforest' %}
{% set linkpool = 'linkpool' %}
{% set linkriver = 'linkriver' %}
{% set simplyvc = 'simplyvc' %}
{% set snzpool = 'snzpool' %}
{% set syncnode = 'syncnode' %}

SELECT
   'ethereum' AS blockchain,
   operator_name,
   keeper_address
FROM (VALUES
  ('{{_01node}}', 0x5428D5Ef94E5998d39613ADCf2f523Cee6f6fb45),
  ('{{_01node}}', 0x4b247AFfe9c4CE6bF05e4A3204ea261288bB4156),
  ('{{alphachain}}', 0xB9B28c3341A88005E421612996BD69A86550909d),
  ('{{alphachain}}', 0x18cBe724E7C248cdA2803F48D1EA6d019623b5cC),
  ('{{anyblock}}', 0x0CE0d98B1FFC1E670e4A68a4B809733884C43174),
  ('{{anyblock}}', 0xa7b2Cf222d287D568E24085E514d4b197759394f),
  ('{{chainlayer}}', 0xaEDcB41619D651dcB0ACF7652127Fc1C66776136),
  ('{{chainlayer}}', 0x72855d64b7EB20379cbd9AB826c0a35DCE33f375),
  ('{{cryptomanufaktur}}', 0x7cb9ff1Ad03DB9D6CCBF99c2A1da872218467612),
  ('{{cryptomanufaktur}}', 0x3824b7a9C6d4Ea93456DF9B07df4fFDB37FFBcbf),
  ('{{dextrac}}', 0x79A4e2666554bF0d47854df60dEaA2636c3E7423),
  ('{{dextrac}}', 0x33512418380F170e5752Fc233F1326f3e805eA62),
  ('{{fiews}}', 0x354B6380aD551B805d4910FF1827FC71102dDBCd),
  ('{{fiews}}', 0x98924befaA16b607b3E168b6C57C9528AF5CC076),
  ('{{inotel}}', 0x8Ad2f5C3e80db1B1e60c13F1D68Fb71807E665DA),
  ('{{inotel}}', 0x083b4cC0DB892160DC5928066D294ba8D4220830),
  ('{{linkforest}}', 0x6E0BD9b8AD35fDc17a6DEf7CF81B5067B7FaA72f),
  ('{{linkforest}}', 0x5C581b8c0961F93543112bf1Ffa27C1cA166e0e5),
  ('{{linkpool}}', 0xFc79eB954321f2e63E29AcbCd46e0e63374923ff),
  ('{{linkpool}}', 0x836cDB9041b442c11c85442A4E5a87aB3dcc0a5F),
  ('{{linkriver}}', 0xd835f40d4719d96E7a000003276786f8Ab50a4A7),
  ('{{linkriver}}', 0xE48f40fBc76cbA315F99Fd5Ba08AfA2f00B8E074),
  ('{{simplyvc}}', 0x77d189d2fD02053978B2E0dc959A6A5536084813),
  ('{{simplyvc}}', 0xF12571de5A310008F1B7490F1aAf52de11325cC8),
  ('{{snzpool}}', 0xaD221f4A2D705CDe03c594beab517CecBDA6727d),
  ('{{snzpool}}', 0x0Fd40853B3B8c7805158b862B76B35A2a27B596A),
  ('{{syncnode}}', 0xA6dBdafa187eDDc4D7A9D8E6B9A2D3F46ee30d24),
  ('{{syncnode}}', 0x86C5d9efB1377DbA0535Cf944Bd6F5736c4290cB)
) a (operator_name, keeper_address)
