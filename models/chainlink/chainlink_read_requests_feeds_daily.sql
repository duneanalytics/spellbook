{{
  config(
    tags=['dunesql'],
    alias=alias('chainlink_read_requests_feeds_daily'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_read_requests_feeds_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        date_month,
        date_start,
        feed_address,
        feed_name,
        total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)