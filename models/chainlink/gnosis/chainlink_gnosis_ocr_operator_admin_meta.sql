{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_admin_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set fiews = 'Fiews' %}
{% set securedatalinks = 'Secure Data Links' %}
{% set mycelium = 'Mycelium' %}
{% set snzpool = 'SNZPool' %}
{% set dextrac = 'DexTrac' %}
{% set 01node = '01Node' %}
{% set chainlayer = 'Chainlayer' %}
{% set anyblockanalytics = 'Anyblock' %}
{% set dmakers = 'dMakers' %}
{% set simplyvc = 'Simply VC' %}
{% set ztake = 'Ztake.org' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set inotel = 'Inotel' %}
{% set linkpool = 'LinkPool' %}

SELECT admin_address, operator_name FROM (VALUES
  ('0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c', '{{chainlayer}}'),
  ('0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03', '{{anyblockanalytics}}'),
  ('0x3FB4600736d306Ee2A89EdF0356D4272fb095768', '{{securedatalinks}}'),
  ('0x3FB4600736d306Ee2A89EdF0356D4272fb095768', '{{mycelium}}'),
  ('0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1', '{{snzpool}}'),
  ('0x9efa0A617C0552F1558c95993aA8b8A68b3e709C', '{{dextrac}}'),
  ('0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B', '{{01node}}'),
  ('0xEaF7dC88d11E81Bb60e3bC5272558041227D16FA', '{{dmakers}}'),
  ('0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf', '{{simplyvc}}'),
  ('0x0039F22efB07A647557C7C5d17854CFD6D489eF3', '{{ztake}}'),
  ('0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03', '{{blockdaemon}}'),
  ('0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4', '{{inotel}}'),
  ('0x797de2909991C66C66D8e730C8385bbab8D18eA6', '{{linkpool}}'),
  ('0x15918ff7f6C44592C81d999B442956B07D26CC44', '{{fiews}}')
) AS tmp_node_meta(admin_address, operator_name)
