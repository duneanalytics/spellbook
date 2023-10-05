{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds'),
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'block_number', 'proxy_address', 'aggregator_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set incremental_interval = '7' %}
{% set project_start_date = '2020-08-29' %}

SELECT 'bnb' as blockchain,
       c.block_time,
       c.block_date,
       c.block_month,
       c.block_number,
       c.feed_name,
       c.oracle_price,
       c.proxy_address,
       c.aggregator_address,
       c.oracle_price / POWER(10, 0) as underlying_token_price,
       CASE 
           WHEN cardinality(split(c.feed_name, ' / ')) = 1 THEN c.feed_name
           ELSE element_at(split(c.feed_name, ' / '), 1)
       END AS base,
       CASE 
           WHEN cardinality(split(c.feed_name, ' / ')) = 1 THEN NULL
           ELSE element_at(split(c.feed_name, ' / '), 2)
       END AS quote
FROM
(
    SELECT
        l.block_time,
        cast(date_trunc('day', l.block_time) as date) as block_date, 
        cast(date_trunc('month', l.block_time) as date) as block_month, 
        l.block_number,
        cfa.feed_name,
        cfa.proxy_address,
        MAX(cfa.aggregator_address) as aggregator_address,
        AVG(
           CAST(bytearray_to_uint256(bytearray_substring(l.topic1, 3, 64)) as DOUBLE) 
           / POWER(10, cfa.decimals)
        ) as oracle_price
    FROM
        {{ source('bnb', 'logs') }} l
    INNER JOIN
        {{ ref('chainlink_bnb_price_feeds_oracle_addresses') }} cfa ON l.contract_address = cfa.aggregator_address
    WHERE
        l.topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
        {% if not is_incremental() %}
        AND l.block_time >= cast('{{project_start_date}}' as date) 
        {% endif %}
        {% if is_incremental() %}
        AND l.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        {% endif %}
    GROUP BY
        1, 2, 3, 4, 5, 6
) c
