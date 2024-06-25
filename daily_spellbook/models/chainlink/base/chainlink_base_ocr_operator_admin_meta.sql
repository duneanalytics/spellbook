{{
  config(
    alias='ocr_operator_admin_meta',
    materialized = 'view'
  )
}}

{% set a01node = '01node' %}
{% set chainlayer = 'chainlayer' %}
{% set cryptomanufaktur = 'cryptoManufaktur' %}
{% set dextrac = 'dexTrac' %}
{% set fiews = 'fiews' %}
{% set linkpool = 'linkPool' %}
{% set matrixedlink = 'matrixedLink' %}
{% set piertwo = 'pierTwo' %}
{% set snzpool = 'snzPool' %}
{% set syncnode = 'syncnode' %}

SELECT admin_address, operator_name FROM (VALUES
  (0x327a666F83E71290760fec367b1939D650895478, '{{a01node}}'),
  (0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c, '{{chainlayer}}'),
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, '{{cryptomanufaktur}}'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, '{{dextrac}}'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, '{{fiews}}'),
  (0xDF812B91D8bf6DF698BFD1D8047839479Ba63420, '{{linkpool}}'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0x4dc81f63CB356c1420D4620414f366794072A3a8, '{{matrixedlink}}'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, '{{piertwo}}'),
  (0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1, '{{snzpool}}'),
  (0xC51D3470693BC049809A1c515606124c7C75908d, '{{syncnode}}')
) AS tmp_node_meta(admin_address, operator_name)
