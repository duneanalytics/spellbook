{{
  config(
    tags=['dunesql'],
    alias=alias('chainlink_read_requests_logs'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_read_requests_logs'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        block_hash,
        block_number,
        block_time,
        tx_hash,
        "from",
        "to",
        input,
        "output"
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)