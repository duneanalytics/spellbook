{{ config(
        alias ='ocr_gas_monthly',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
        )
}}

{% set models = [
 'chainlink_ethereum_ocr_gas_monthly'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_date,
      node_address,
      node_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      total_token_amount,
      total_usd_amount        
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)