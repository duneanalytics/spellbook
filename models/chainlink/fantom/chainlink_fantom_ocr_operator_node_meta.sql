{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set chainlayer = 'Chainlayer' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dextrac = 'DexTrac' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set kytzu = 'Kytzu' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set newroad = 'Newroad Network' %}
{% set p2porg = 'P2P.org' %}
{% set prophet = 'Prophet' %}
{% set syncnode = 'SyncNode' %}
{% set tiingo = 'Tiingo' %}
{% set vulcan = 'Vulcan Link' %}

SELECT node_address, operator_name FROM (VALUES
  (0x09285FBb87B75FBA9400683233C0BC1DE53afCa8, '{{a01node}}'),
  (0x139a1fD7230E215346cd583f48d87aedc4276B45, '{{a01node}}'),
  (0xCC4c922db2EF8c911F37E73c03B632DD1585Ad0E, '{{chainlayer}}'),
  (0xB2ffBb538558196e5Db351b33B647eFe654a9647, '{{cryptomanufaktur}}'),
  (0xF3C4a8041c62146350C83d593D4ecccD3C1D8F86, '{{dextrac}}'),
  (0x05Ee5882122A86C8D15D8D5ECB42830503A7d0d8, '{{easy2stake}}'),
  (0x31C67a7132DB4347CAe91B93427C27E2d9a2d624, '{{fiews}}'),
  (0x13bd632d6Ff4Cb8738172A3b43BdEbf618E580C0, '{{inotel}}'),
  (0x9482f1C1a7Be7376373cc17274c573085059bFEE, '{{kytzu}}'),
  (0xE9300E1dADA80A7178B14F3980616D2657e706d5, '{{linkforest}}'),
  (0x89123981e8bcc9532A8c3f6C6EF5ad08A67eFA1d, '{{linkpool}}'),
  (0x9C53F322F40C90A978E282850dFb445EE7a46975, '{{linkriver}}'),
  (0x0C37dFf392f202447946fe7F45f950b5E456A13a, '{{newroad}}'),
  (0x120Af64d9B7bB555cd2Abc47A945d126ddeD0376, '{{p2porg}}'),
  (0x8A8530344e4ABd4C4C34E4DB68BA88C8Bea69254, '{{prophet}}'),
  (0x7dAeEC3B738C130ea78d4EaBDCE3b791c44555db, '{{syncnode}}'),
  (0x991340a2A87db4397339e913E7bBdc1847b61414, '{{tiingo}}'),
  (0xC87DD1D817018102B313514E497293E8878795d8, '{{vulcan}}')
) AS tmp_node_meta(node_address, operator_name)
