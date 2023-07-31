{{
      config(
        tags=['dunesql'],
        alias='ocr_operator_admin_meta',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "chainlink",
                                    \'["linkpool_ryan", "linkpool_jon]\') }}'
      )
    }}
{% set northwestnodes = 'NorthWest Nodes' %}
{% set fiews = 'Fiews' %}
{% set matrixedlink = 'Matrixed.Link' %}
{% set vulcan = 'Vulcan Link' %}
{% set chainlayer = 'Chainlayer' %}
{% set dextrac = 'DexTrac' %}
{% set inotel = 'Inotel' %}
{% set frameworkventures = 'Framework Ventures' %}
{% set simplyvc = 'Simply VC' %}
{% set linkforest = 'LinkForest' %}
{% set mycelium = 'Mycelium' %}
{% set linkriver = 'LinkRiver' %}
{% set linkpool = 'LinkPool' %}
{% set validationcloud = 'Validation Cloud' %}
{% set snzpool = 'SNZPool' %}
{% set validationcapital = 'Validation Capital' %}
SELECT admin_address, operator_name FROM (VALUES
  ('0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf', '{{simplyvc}}'),
  ('0x6eF38c3d1D85B710A9e160aD41B912Cb8CAc2589', '{{frameworkventures}}'),
  ('0x14f94049397C3F1807c45B6f854Cb5F36bC4393B', '{{linkriver}}'),
  ('0x797de2909991C66C66D8e730C8385bbab8D18eA6', '{{linkpool}}'),
  ('0x183A96629fF566e7AA8AfA38980Cd037EB40A59A', '{{validationcloud}}'),
  ('0x0446B8d5d3F3fA74eDbd32154b023FD8da172f05', '{{snzpool}}'),
  ('0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1', '{{snzpool}}'),
  ('0x183A96629fF566e7AA8AfA38980Cd037EB40A59A', '{{validationcapital}}'),
  ('0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e', '{{linkforest}}'),
  ('0x3FB4600736d306Ee2A89EdF0356D4272fb095768', '{{mycelium}}'),
  ('0x0921E157b690c4F89F7C2a210cFd8bF3964F6776', '{{northwestnodes}}'),
  ('0x4E28977d71f148ae2c523e8Aa4b6F3071d81Add1', '{{vulcan}}'),
  ('0x7D0f8dd25135047967bA6C50309b567957dd52c3', '{{vulcan}}'),
  ('0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c', '{{chainlayer}}'),
  ('0x9efa0A617C0552F1558c95993aA8b8A68b3e709C', '{{dextrac}}'),
  ('0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4', '{{inotel}}'),
  ('0x15918ff7f6C44592C81d999B442956B07D26CC44', '{{fiews}}'),
  ('0x4dc81f63CB356c1420D4620414f366794072A3a8', '{{matrixedlink}}'),
) AS tmp_node_meta(admin_address, operator_name)
