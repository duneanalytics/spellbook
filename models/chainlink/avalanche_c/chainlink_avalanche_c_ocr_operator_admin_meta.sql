{{
      config(
        tags=['dunesql'],
        alias='ocr_operator_admin_meta',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "chainlink",
                                    \'["linkpool_ryan", "linkpool_jon]\') }}'
      )
    }}
{% set northwestnodes = 'NorthWest Nodes' %}
{% set fiews = 'Fiews' %}
{% set linkriver = 'LinkRiver' %}
{% set alphachain = 'Alpha Chain' %}
{% set inotel = 'Inotel' %}
{% set linkpool = 'LinkPool' %}
{% set anyblockanalytics = 'Anyblock' %}
{% set simplyvc = 'Simply VC' %}
{% set validationcloud = 'Validation Cloud' %}
{% set blocksizecapital = 'Blocksize Capital' %}
{% set dextrac = 'DexTrac' %}
{% set validationcapital = 'Validation Capital' %}
{% set lexisnexis = 'LexisNexis' %}
{% set linkforest = 'LinkForest' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set chainlayer = 'Chainlayer' %}
{% set 01node = '01Node' %}
{% set p2porg = 'P2P.org' %}
SELECT admin_address, operator_name FROM (VALUES
  ('0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf', '{{simplyvc}}'),
  ('0x183A96629fF566e7AA8AfA38980Cd037EB40A59A', '{{validationcloud}}'),
  ('0x7CC60c9C24E9A290Db55b1017AF477E5c87a7550', '{{blocksizecapital}}'),
  ('0x797de2909991C66C66D8e730C8385bbab8D18eA6', '{{linkpool}}'),
  ('0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03', '{{anyblockanalytics}}'),
  ('0xD290AA3098882ccAdEeec86F6857d3CFA29BCf3b', '{{lexisnexis}}'),
  ('0x098a4C7ceCbfb8534e5Ab3f9c8F6C87845Fc5109', '{{lexisnexis}}'),
  ('0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e', '{{linkforest}}'),
  ('0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03', '{{blockdaemon}}'),
  ('0x4a3dF8cAe46765d33c2551ff5438a5C5FC44347c', '{{chainlayer}}'),
  ('0x9efa0A617C0552F1558c95993aA8b8A68b3e709C', '{{dextrac}}'),
  ('0x183A96629fF566e7AA8AfA38980Cd037EB40A59A', '{{validationcapital}}'),
  ('0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B', '{{01node}}'),
  ('0xCDa423ee5A7A886eF113b181469581306fC8B607', '{{p2porg}}'),
  ('0x14f94049397C3F1807c45B6f854Cb5F36bC4393B', '{{linkriver}}'),
  ('0xa5D0084A766203b463b3164DFc49D91509C12daB', '{{alphachain}}'),
  ('0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4', '{{inotel}}'),
  ('0x0921E157b690c4F89F7C2a210cFd8bF3964F6776', '{{northwestnodes}}'),
  ('0x15918ff7f6C44592C81d999B442956B07D26CC44', '{{fiews}}'),
) AS tmp_node_meta(admin_address, operator_name)
