{{
  config(
    
    alias='price_feeds_hourly',
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'hour', 'proxy_address', 'aggregator_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\', "project", "chainlink", \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set incremental_interval = '7' %}
{% set project_start_date = '2019-10-01' %}

WITH hourly_sequence_meta AS (
    SELECT
      date_trunc('hour', price.minute) as hour
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      price.symbol = 'LINK'
      {% if not is_incremental() %}
        AND price.minute >= timestamp '{{project_start_date}}'
      {% endif %}
      {% if is_incremental() %}
        AND price.minute >= date_trunc('hour', now() - interval '{{incremental_interval}}' day)
      {% endif %}
    GROUP BY
      1
    ORDER BY
      1
),

hourly_sequence AS (
    SELECT
        oracle_addresses.hr,
        oracle_addresses.feed_name,
        oracle_addresses.proxy_address,
        oracle_addresses.aggregator_address
    FROM (
        SELECT
          hourly_sequence_meta.hour as hr,
          feed_name,
          proxy_address,
          aggregator_address
        FROM {{ ref('chainlink_arbitrum_price_feeds_oracle_addresses') }}
        CROSS JOIN hourly_sequence_meta
    ) oracle_addresses
),

aggregated_price_feeds AS (
    SELECT
        hourly_sequence.hr AS hour,
        hourly_sequence.feed_name,
        hourly_sequence.proxy_address,
        hourly_sequence.aggregator_address,
        AVG(price_feeds.oracle_price) AS oracle_price_avg,
        AVG(price_feeds.underlying_token_price) AS underlying_token_price_avg,
        MAX(price_feeds.quote) AS quote,
        MAX(price_feeds.base) AS base
    FROM hourly_sequence
    LEFT JOIN {{ ref('chainlink_arbitrum_price_feeds') }} price_feeds
        ON hourly_sequence.hr = date_trunc('hour', price_feeds.block_time)
        AND hourly_sequence.proxy_address = price_feeds.proxy_address
        AND hourly_sequence.aggregator_address = price_feeds.aggregator_address
    WHERE
        {% if not is_incremental() %}
          hourly_sequence.hr >= timestamp '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
          hourly_sequence.hr >= date_trunc('hour', now() - interval '{{incremental_interval}}' day)
        {% endif %}
    GROUP BY
        hourly_sequence.hr, hourly_sequence.feed_name, hourly_sequence.proxy_address, hourly_sequence.aggregator_address
)

SELECT
    'arbitrum' AS blockchain,
    hour,
    cast(date_trunc('day', hour) as date) AS block_date,
    cast(date_trunc('month', hour) as date) AS block_month,
    feed_name,
    proxy_address,
    aggregator_address,
    oracle_price_avg,
    underlying_token_price_avg,
    base,
    quote
FROM aggregated_price_feeds
WHERE oracle_price_avg IS NOT NULL
