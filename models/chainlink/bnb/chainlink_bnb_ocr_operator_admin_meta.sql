{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_admin_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT admin_address, operator_name
FROM (VALUES 
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0x97b7CF748f1eb0B451f4464B4Aebc639d18Ddb48, '{{linkpool}}')
) AS tmp_node_meta(admin_address, operator_name)
