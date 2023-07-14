{{
  config(
    tags=['dunesql'],
    alias='ocr_operator_node_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set linkpool = 'LinkPool' %}

SELECT node_address, operator_name
FROM (VALUES 
  (0x37Fc26312b831f7efb494cDB192c9aE91Fd27597, '{{linkpool}}'),
  (0x3B5398B508a26b43822456b0D3Ad78B649011dA6, '{{linkpool}}')
) AS tmp_node_meta(node_address, operator_name)


