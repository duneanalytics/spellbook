{{ config(
        alias ='ocr_request_daily',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
        )
}}

{% set models = [
 'chainlink_ethereum_ocr_request_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_date,
      node_address,
      node_name,
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)