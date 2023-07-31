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
  ('0xbD620be125abf8b569B9A3CC132aad0bcF1Ff0E7', '{{mycelium}}'),
  ('0xb6d51122E3473a2A463f2F8752660570237C30a4', '{{linkriver}}'),
  ('0x27C56A6d40F20A33349F0822201fbc10d455Be66', '{{linkpool}}'),
  ('0xB0576808343819a21B9171B018b87da204967B6F', '{{validationcloud}}'),
  ('0x01f4e56D5ee46e84Edf8595ca7A7B62a3306De76', '{{snzpool}}'),
  ('0xB0576808343819a21B9171B018b87da204967B6F', '{{validationcapital}}'),
  ('0x51c5C8e2267582562f6cF08cF641A1A174946A50', '{{linkforest}}'),
  ('0xEB1a8834cF6CA6d721E5CB1A8Ad472BBF62eEf8E', '{{northwestnodes}}'),
  ('0x64AE501217d502Be8a1d9D4a4f669fbAC6a0c062', '{{matrixedlink}}'),
  ('0x3cae103213dB7673072E138A622bD17b20bc7ad4', '{{vulcan}}'),
  ('0x1a6d5C4396EaF8ED93Ec77bf1aF9B43ffeD7814d', '{{chainlayer}}'),
  ('0x5616CAABa92cdf656E7d1bA36Fe1bd878E51c174', '{{dextrac}}'),
  ('0xC7310123914f624Da9C376f8eC590055e62733c1', '{{dextrac}}'),
  ('0xC76b9d4B13717f959ea45Ec6e3Db9C3F9304d7d5', '{{inotel}}'),
  ('0xA82d4EdB72dD3D167D00058F2404658F4E9A010A', '{{fiews}}'),
  ('0xD596389948247b582317a1EfA76cD7741A134191', '{{simplyvc}}'),
  ('0xe870848FEb433FBE423b08791c44D8bf31B2D4Dc', '{{frameworkventures}}'),
) AS tmp_node_meta(admin_address, operator_name)
