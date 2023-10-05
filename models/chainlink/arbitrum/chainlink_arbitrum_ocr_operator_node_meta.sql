{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set chainlayer = 'Chainlayer' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set frameworkventures = 'Framework Ventures' %}
{% set inotel = 'Inotel' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set matrixedlink = 'Matrixed.Link' %}
{% set northwestnodes = 'NorthWest Nodes' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set validationcloud = 'Validation Cloud' %}

SELECT node_address, operator_name FROM (VALUES
  (0x1a6d5C4396EaF8ED93Ec77bf1aF9B43ffeD7814d, '{{chainlayer}}'),
  (0x5616CAABa92cdf656E7d1bA36Fe1bd878E51c174, '{{dextrac}}'),
  (0xC7310123914f624Da9C376f8eC590055e62733c1, '{{dextrac}}'),
  (0xA82d4EdB72dD3D167D00058F2404658F4E9A010A, '{{fiews}}'),
  (0xe870848FEb433FBE423b08791c44D8bf31B2D4Dc, '{{frameworkventures}}'),
  (0xC76b9d4B13717f959ea45Ec6e3Db9C3F9304d7d5, '{{inotel}}'),
  (0x51c5C8e2267582562f6cF08cF641A1A174946A50, '{{linkforest}}'),
  (0x27C56A6d40F20A33349F0822201fbc10d455Be66, '{{linkpool}}'),
  (0xb6d51122E3473a2A463f2F8752660570237C30a4, '{{linkriver}}'),
  (0x64AE501217d502Be8a1d9D4a4f669fbAC6a0c062, '{{matrixedlink}}'),
  (0xEB1a8834cF6CA6d721E5CB1A8Ad472BBF62eEf8E, '{{northwestnodes}}'),
  (0xbD620be125abf8b569B9A3CC132aad0bcF1Ff0E7, '{{piertwo}}'),
  (0xD596389948247b582317a1EfA76cD7741A134191, '{{simplyvc}}'),
  (0x01f4e56D5ee46e84Edf8595ca7A7B62a3306De76, '{{snzpool}}'),
  (0xB0576808343819a21B9171B018b87da204967B6F, '{{validationcloud}}'),
  (0x3cae103213dB7673072E138A622bD17b20bc7ad4, '{{validationcloud}}')
) AS tmp_node_meta(node_address, operator_name)
