{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_hourly'),
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'hour', 'proxy_address', 'underlying_token_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}
{% set project_start_date = '2020-08-29' %}

WITH
  hourly_sequence_meta as (
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
        oracle_addresses.aggregator_address,
        token_mapping.underlying_token_address
    FROM (
        SELECT
          hourly_sequence_meta.hour as hr,
          feed_name,
          proxy_address,
          aggregator_address
        FROM {{ ref('chainlink_bnb_price_feeds_oracle_addresses') }}
        CROSS JOIN hourly_sequence_meta
    ) oracle_addresses 
    LEFT JOIN {{ ref('chainlink_bnb_price_feeds_oracle_token_mapping') }} token_mapping 
    ON token_mapping.proxy_address = oracle_addresses.proxy_address
  )
SELECT 
    'bnb' AS blockchain,
    hour,
    cast(date_trunc('day', hour) as date) as block_date,
    cast(date_trunc('month', hour) as date) as block_month,
    feed_name,
    proxy_address,
    aggregator_address,
    underlying_token_address,
    oracle_price_avg,
    underlying_token_price_avg
FROM (
    SELECT
        hr AS hour,
        feed_name,
        proxy_address,
        aggregator_address,
        underlying_token_address,
        FIRST_VALUE(oracle_price_avg) 
            OVER (
                PARTITION BY feed_name, 
                             proxy_address,
                             aggregator_address,
                             underlying_token_address,
                             grp 
                ORDER BY hr
            ) AS oracle_price_avg,
        FIRST_VALUE(underlying_token_price_avg) 
            OVER (
                PARTITION BY feed_name,
                             proxy_address,
                             aggregator_address,
                             underlying_token_address,
                             grp
                ORDER BY hr
            ) AS underlying_token_price_avg
    FROM
    (
        SELECT
            hr,
            feed_name,
            proxy_address,
            aggregator_address,
            underlying_token_address,
            underlying_token_price_avg,
            oracle_price_avg,
            COUNT(oracle_price_avg) 
                OVER (
                    PARTITION BY feed_name,
                                 proxy_address,
                                 aggregator_address,
                                 underlying_token_address
                    ORDER BY hr
                ) AS grp
        FROM (
            SELECT 
                hourly_sequence.hr,
                hourly_sequence.feed_name,
                hourly_sequence.proxy_address,
                hourly_sequence.aggregator_address,
                hourly_sequence.underlying_token_address,
                AVG(price_feeds.oracle_price) AS oracle_price_avg,
                AVG(price_feeds.underlying_token_price) AS underlying_token_price_avg
            FROM hourly_sequence 
            LEFT JOIN {{ ref('chainlink_bnb_price_feeds') }} price_feeds 
            ON hourly_sequence.hr = date_trunc('hour', price_feeds.block_time)
            AND hourly_sequence.underlying_token_address = price_feeds.underlying_token_address
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
                1, 2, 3, 4, 5
        ) avg_prices
    ) price_groups
) price_with_non_null_avg
WHERE oracle_price_avg IS NOT NULL