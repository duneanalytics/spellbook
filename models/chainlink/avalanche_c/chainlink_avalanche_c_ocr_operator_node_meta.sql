{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0x69Dd5981be8F53828b9A305666F91133dfc0FdD2, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


