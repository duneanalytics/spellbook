{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0x11eB6a69A56DF3a89b99c4b1484691Af4AB0c508, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


