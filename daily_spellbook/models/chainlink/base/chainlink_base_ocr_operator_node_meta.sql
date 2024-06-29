{{
  config(
    alias='ocr_operator_node_meta',
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

SELECT node_address, operator_name FROM (VALUES
  (0x34816bc1f605E0d58d46C43f1584C3A84F811e85, '{{a01node}}'),
  (0xc04a3C4aBF8995Da051140f552Cb4eB086185836, '{{chainlayer}}'),
  (0x58F7B80BE97D031bd1067A2acdcF3Ce100790019, '{{cryptomanufaktur}}'),
  (0x87b49edBBc2cCe9237276C0D9F03d59d278dd1eB, '{{dextrac}}'),
  (0x371DFBC7575012BB28D3709A8918A47464b1b7DC, '{{fiews}}'),
  (0xD6F8632e7Cdca416CD64f3ac4F286887165E1D74, '{{linkpool}}'),
  (0x02E9137940be6803D333EB1451C3834C96cA9C2d, '{{matrixedlink}}'),
  (0xe493145aDcF22C4EFE07D720401c7ce9961d70Ea, '{{piertwo}}'),
  (0xAbFa05C981f49f8d42D9a3361D53924Df2c64966, '{{snzpool}}'),
  (0xB9e44696B045ab005eA956253d8676F656eEBC60, '{{syncnode}}')
) AS tmp_node_meta(node_address, operator_name)
