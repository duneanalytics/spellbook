{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set alphachain = 'Alpha Chain' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set blocksizecapital = 'Blocksize Capital' %}
{% set chainlayer = 'Chainlayer' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set lexisnexis = 'LexisNexis' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set northwestnodes = 'NorthWest Nodes' %}
{% set p2porg = 'P2P.org' %}
{% set simplyvc = 'Simply VC' %}
{% set validationcloud = 'Validation Cloud' %}
{% set vodafone = 'Vodafone' %}

SELECT node_address, operator_name FROM (VALUES
  (0x7666D648f6D0909DbCb010218d713c5CE576149A, '{{a01node}}'),
  (0xafbB53C92ED3f8B875C37ca838d39dd7D962eDa0, '{{a01node}}'),
  (0x89da56e409dDef3C52BdCfBeFC9b595870880bAA, '{{alphachain}}'),
  (0x5b17Db9668BB9CbFe87F416b4da1132b5193959D, '{{blockdaemon}}'),
  (0xe5b37dc608C73852F9c0f56E30f8d74D89b51C55, '{{blocksizecapital}}'),
  (0xd877d01d972D28dBd28ed138c63173D07A024E5C, '{{chainlayer}}'),
  (0x8772Ecc810a04627d58eBC1Db2bBc27bF90F6bb2, '{{dextrac}}'),
  (0x0499BEA33347cb62D79A9C0b1EDA01d8d329894c, '{{fiews}}'),
  (0x8EF62f0B88286AAF46142cacBB0058bf1F74607d, '{{inotel}}'),
  (0xD59D1073d5A1B77d4Cf36C6D4a287DDF7F67F348, '{{lexisnexis}}'),
  (0x574A2f48049D962cF2e66d4381823Af93817Cd81, '{{linkforest}}'),
  (0x69Dd5981be8F53828b9A305666F91133dfc0FdD2, '{{linkpool}}'),
  (0x00B1943fFB046aC69E9618361f1eAd3ccE112fEa, '{{linkriver}}'),
  (0xa6cd0E363740069e4570a0dA03e7258108B399Ab, '{{northwestnodes}}'),
  (0xfDA44C0BaE0ACFa9eEAaB91d6C103eDaE6001876, '{{p2porg}}'),
  (0xFb821dfde8F43ed6fbf970153585038b0b3B49CC, '{{simplyvc}}'),
  (0xA317eBD3dA5C29b6EA01742bbEa6BaCCEB10A297, '{{validationcloud}}'),
  (0x2B0a62d7aFed4eF6AfAdA2491Ae2c7539D9e35E7, '{{vodafone}}')
) AS tmp_node_meta(node_address, operator_name)
