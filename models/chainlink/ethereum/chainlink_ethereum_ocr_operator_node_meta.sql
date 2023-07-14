{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0xcC29be4Ca92D4Ecc43C8451fBA94C200B83991f6, '{{linkpool}}'),
  (0x1589d072aC911a55c2010D97839a1f61b1e3323A, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


