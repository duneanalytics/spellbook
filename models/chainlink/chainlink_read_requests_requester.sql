{{
  config(
    tags=['dunesql'],
    alias=alias('chainlink_read_requests_requester'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_read_requests_requester'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        date_start,
        feed_address,
        feed_name,
        requester_address,
        requester_name
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)