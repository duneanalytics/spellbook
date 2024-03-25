{{
  config(
    
    alias='ocr_operator_admin_meta',
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

SELECT admin_address, operator_name FROM (VALUES
  (0xC9110BebfC02fCa90a1cc3417F8869784f63B94B, '{{a01node}}'),
  (0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B, '{{a01node}}'),
  (0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03, '{{blockdaemon}}'),
  (0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c, '{{chainlayer}}'),
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, '{{cryptomanufaktur}}'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, '{{dextrac}}'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, '{{fiews}}'),
  (0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4, '{{inotel}}'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0xDF812B91D8bf6DF698BFD1D8047839479Ba63420, '{{linkpool}}'),
  (0x14f94049397C3F1807c45B6f854Cb5F36bC4393B, '{{linkriver}}'),
  (0x69F89eFbB5e5519EAf93a0Af3dbA3f3101350b0d, '{{linkriver}}'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, '{{piertwo}}'),
  (0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf, '{{simplyvc}}')
) AS tmp_node_meta(admin_address, operator_name)
