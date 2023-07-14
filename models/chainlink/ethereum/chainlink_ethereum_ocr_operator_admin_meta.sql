{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_admin_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT admin_address, operator_name
FROM (VALUES 
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0xe9e11963f61322299f9919ff1dda01a825e82dbc, '{{linkpool}}')
) AS tmp_node_meta(admin_address, operator_name)
