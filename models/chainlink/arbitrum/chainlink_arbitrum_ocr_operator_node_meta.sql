{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0x27C56A6d40F20A33349F0822201fbc10d455Be66, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


