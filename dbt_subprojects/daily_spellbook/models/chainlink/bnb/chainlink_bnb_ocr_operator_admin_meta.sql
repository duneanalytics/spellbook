{{
  config(
    
    alias='ocr_operator_admin_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set alphachain = 'Alpha Chain' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set chainlayer = 'Chainlayer' %}
{% set cosmostation = 'Cosmostation' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set dxfeed = 'dxFeed' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set fiews = 'Fiews' %}
{% set frameworkventures = 'Framework Ventures' %}
{% set inotel = 'Inotel' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set onchaintech = 'On-chain Tech' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set syncnode = 'SyncNode' %}
{% set tiingo = 'Tiingo' %}
{% set validationcloud = 'Validation Cloud' %}
{% set xbto = 'XBTO' %}
{% set ztake = 'Ztake.org' %}

SELECT admin_address, operator_name FROM (VALUES
  (0x56f770Ae39d6174fCDC1929C5B85baC8de47e74F, '{{a01node}}'),
  (0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B, '{{a01node}}'),
  (0xa5D0084A766203b463b3164DFc49D91509C12daB, '{{alphachain}}'),
  (0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03, '{{blockdaemon}}'),
  (0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c, '{{chainlayer}}'),
  (0x1B17eB8FAE3C28CB2463235F9D407b527ba4e6Dd, '{{cosmostation}}'),
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, '{{cryptomanufaktur}}'),
  (0xB98DA55e3E72BabF18c4f421Ea5B653519e79f2B, '{{dmakers}}'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, '{{dextrac}}'),
  (0xb284a468522663F6219f2912ca10145B52b13503, '{{dxfeed}}'),
  (0x991812566f6E14897Fc1e401D24de19845c0442f, '{{dxfeed}}'),
  (0xFdC770353dC0bFCE80a17Ab8a6a2E7d80590f1Ba, '{{easy2stake}}'),
  (0x38a75E2A093d8F9b815AAE9cA6A5Eb0c2901329b, '{{fiews}}'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, '{{fiews}}'),
  (0x6eF38c3d1D85B710A9e160aD41B912Cb8CAc2589, '{{frameworkventures}}'),
  (0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4, '{{inotel}}'),
  (0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e, '{{linkforest}}'),
  (0xD48fc6E2B73C2988fA50C994181C0CdCa850D62a, '{{linkforest}}'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0x97b7CF748f1eb0B451f4464B4Aebc639d18Ddb48, '{{linkpool}}'),
  (0xDF812B91D8bf6DF698BFD1D8047839479Ba63420, '{{linkpool}}'),
  (0x14f94049397C3F1807c45B6f854Cb5F36bC4393B, '{{linkriver}}'),
  (0x35DaC078fC9E6e45d89a6CBc78A776BA719b485D, '{{onchaintech}}'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, '{{piertwo}}'),
  (0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf, '{{simplyvc}}'),
  (0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1, '{{snzpool}}'),
  (0xC51D3470693BC049809A1c515606124c7C75908d, '{{syncnode}}'),
  (0xfAE26207ab74ee528214ee92f94427f8Cdbb6A32, '{{tiingo}}'),
  (0x183A96629fF566e7AA8AfA38980Cd037EB40A59A, '{{validationcloud}}'),
  (0x0b16EC1044F60F03B0e815f863bd4d27638cbD0A, '{{xbto}}'),
  (0xBa5ed4Cd392ABC8Df7009B4A385eB3e05c7375F1, '{{ztake}}'),
  (0x0039F22efB07A647557C7C5d17854CFD6D489eF3, '{{ztake}}'),
  (0x9d69B0fcbcf9a7e513E947Cd7ce2019904e2E764, '{{ztake}}')
) AS tmp_node_meta(admin_address, operator_name)
