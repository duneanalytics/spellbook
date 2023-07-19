{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0xa1ab1c841898Fe94900d00d9312ba954e4F81501, '{{linkpool}}'),
  (0x9F9922d4bBa463EfBBcF8563282723d98587f7eb, '{{linkpool}}'),
  (0xf03b7095B089A4e601fB13F2BF6af518eb199a0b, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


