{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0x89123981e8bcc9532A8c3f6C6EF5ad08A67eFA1d, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


