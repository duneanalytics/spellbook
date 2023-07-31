{{
      config(
        tags=['dunesql'],
        alias='ocr_operator_admin_meta',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "chainlink",
                                    \'["linkpool_ryan", "linkpool_jon]\') }}'
      )
    }}
{% set linkpool = 'LinkPool' %}
{% set 01node = '01Node' %}
{% set inotel = 'Inotel' %}
{% set chainlayer = 'Chainlayer' %}
{% set newroad = 'Newroad Network' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set tiingo = 'Tiingo' %}
{% set p2porg = 'P2P.org' %}
{% set dextrac = 'DexTrac' %}
{% set linkriver = 'LinkRiver' %}
{% set kytzu = 'Kytzu' %}
{% set syncnode = 'SyncNode' %}
{% set linkforest = 'LinkForest' %}
{% set prophet = 'Prophet' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set fiews = 'Fiews' %}
{% set vulcan = 'Vulcan Link' %}
SELECT admin_address, operator_name FROM (VALUES
  ('0xfAE26207ab74ee528214ee92f94427f8Cdbb6A32', '{{tiingo}}'),
  ('0xCDa423ee5A7A886eF113b181469581306fC8B607', '{{p2porg}}'),
  ('0xBDB624CD1051F687f116bB0c642330B2aBdfcc06', '{{prophet}}'),
  ('0x9efa0A617C0552F1558c95993aA8b8A68b3e709C', '{{dextrac}}'),
  ('0x14f94049397C3F1807c45B6f854Cb5F36bC4393B', '{{linkriver}}'),
  ('0x57F7f85C151A8A96CC20fEa6a43622334C335fe4', '{{kytzu}}'),
  ('0x001E0d294383d5b4136476648aCc8D04a6461Ae3', '{{kytzu}}'),
  ('0xC51D3470693BC049809A1c515606124c7C75908d', '{{syncnode}}'),
  ('0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e', '{{linkforest}}'),
  ('0x59eCf48345A221E0731E785ED79eD40d0A94E2A5', '{{cryptomanufaktur}}'),
  ('0x15918ff7f6C44592C81d999B442956B07D26CC44', '{{fiews}}'),
  ('0x7D0f8dd25135047967bA6C50309b567957dd52c3', '{{vulcan}}'),
  ('0xFdC770353dC0bFCE80a17Ab8a6a2E7d80590f1Ba', '{{easy2stake}}'),
  ('0x797de2909991C66C66D8e730C8385bbab8D18eA6', '{{linkpool}}'),
  ('0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B', '{{01node}}'),
  ('0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4', '{{inotel}}'),
  ('0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c', '{{chainlayer}}'),
  ('0xAB35418fB9f8B13E3e6857c36A0769b9F94a87EC', '{{newroad}}'),
) AS tmp_node_meta(admin_address, operator_name)
