{{
  config(
    
    alias='ocr_operator_admin_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set alphachain = 'Alphachain' %}
{% set bharvest = 'B Harvest' %}
{% set blocksizecapital = 'BlocksizeCapital' %}
{% set chainlayer = 'Chainlayer' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set matrixedlink = 'Matrixed.Link' %}
{% set newroad = 'Newroad Network' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set stakingfacilities = 'Staking Facilities' %}
{% set vulcan = 'Vulcan Link' %}
{% set ztake = 'Ztake.org' %}

SELECT admin_address, operator_name FROM (VALUES
  (0xEcE9E7521451e2E8dEe06C1677CF36274585377f, '{{a01node}}'),
  (0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B, '{{a01node}}'),
  (0xa5D0084A766203b463b3164DFc49D91509C12daB, '{{alphachain}}'),
  (0x6cDC3Efa3bAa392fAF3E5c1Ca802E15B6185E0e8, '{{bharvest}}'),
  (0x7CC60c9C24E9A290Db55b1017AF477E5c87a7550, '{{blocksizecapital}}'),
  (0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c, '{{chainlayer}}'),
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, '{{cryptomanufaktur}}'),
  (0xB9e62F6a14aC8BabB7f99993bdc3182a1976c22E, '{{dmakers}}'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, '{{dextrac}}'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, '{{fiews}}'),
  (0xB97a32D95A31a504C3dB28dDd574F21c700EDbee, '{{fiews}}'),
  (0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4, '{{inotel}}'),
  (0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e, '{{linkforest}}'),
  (0xD56FBFF05D2e1cdbeb5CB50e8055dAD0cf864792, '{{linkforest}}'),
  (0xD48fc6E2B73C2988fA50C994181C0CdCa850D62a, '{{linkforest}}'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0xDF812B91D8bf6DF698BFD1D8047839479Ba63420, '{{linkpool}}'),
  (0x14f94049397C3F1807c45B6f854Cb5F36bC4393B, '{{linkriver}}'),
  (0x4dc81f63CB356c1420D4620414f366794072A3a8, '{{matrixedlink}}'),
  (0xAB35418fB9f8B13E3e6857c36A0769b9F94a87EC, '{{newroad}}'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, '{{piertwo}}'),
  (0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf, '{{simplyvc}}'),
  (0x1f11134A80aEd1FF47E3ee97A4d3f978A0629669, '{{simplyvc}}'),
  (0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1, '{{snzpool}}'),
  (0x3D65be029c949F52cABa2d8E8270c098256697d9, '{{stakingfacilities}}'),
  (0x7D0f8dd25135047967bA6C50309b567957dd52c3, '{{vulcan}}'),
  (0x0039F22efB07A647557C7C5d17854CFD6D489eF3, '{{ztake}}')
) AS tmp_node_meta(admin_address, operator_name)
