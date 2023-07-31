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
{% set linkforest = 'LinkForest' %}
{% set prophet = 'Prophet' %}
{% set dextrac = 'DexTrac' %}
{% set linkriver = 'LinkRiver' %}
{% set kytzu = 'Kytzu' %}
{% set syncnode = 'SyncNode' %}
{% set vulcan = 'Vulcan Link' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set fiews = 'Fiews' %}
{% set newroad = 'Newroad Network' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set linkpool = 'LinkPool' %}
{% set 01node = '01Node' %}
{% set inotel = 'Inotel' %}
{% set chainlayer = 'Chainlayer' %}
{% set tiingo = 'Tiingo' %}
{% set p2porg = 'P2P.org' %}
SELECT admin_address, operator_name FROM (VALUES
  ('0x7dAeEC3B738C130ea78d4EaBDCE3b791c44555db', '{{syncnode}}'),
  ('0xE9300E1dADA80A7178B14F3980616D2657e706d5', '{{linkforest}}'),
  ('0x8A8530344e4ABd4C4C34E4DB68BA88C8Bea69254', '{{prophet}}'),
  ('0xF3C4a8041c62146350C83d593D4ecccD3C1D8F86', '{{dextrac}}'),
  ('0x9C53F322F40C90A978E282850dFb445EE7a46975', '{{linkriver}}'),
  ('0x9482f1C1a7Be7376373cc17274c573085059bFEE', '{{kytzu}}'),
  ('0x31C67a7132DB4347CAe91B93427C27E2d9a2d624', '{{fiews}}'),
  ('0xC87DD1D817018102B313514E497293E8878795d8', '{{vulcan}}'),
  ('0xB2ffBb538558196e5Db351b33B647eFe654a9647', '{{cryptomanufaktur}}'),
  ('0xCC4c922db2EF8c911F37E73c03B632DD1585Ad0E', '{{chainlayer}}'),
  ('0x0C37dFf392f202447946fe7F45f950b5E456A13a', '{{newroad}}'),
  ('0x05Ee5882122A86C8D15D8D5ECB42830503A7d0d8', '{{easy2stake}}'),
  ('0x89123981e8bcc9532A8c3f6C6EF5ad08A67eFA1d', '{{linkpool}}'),
  ('0x09285FBb87B75FBA9400683233C0BC1DE53afCa8', '{{01node}}'),
  ('0x13bd632d6Ff4Cb8738172A3b43BdEbf618E580C0', '{{inotel}}'),
  ('0x120Af64d9B7bB555cd2Abc47A945d126ddeD0376', '{{p2porg}}'),
  ('0x991340a2A87db4397339e913E7bBdc1847b61414', '{{tiingo}}'),
) AS tmp_node_meta(admin_address, operator_name)
