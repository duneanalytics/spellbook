{{
  config(
	tags=['legacy', 'remove'],
    alias=alias('price_feeds', legacy_model=True),
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'block_number', 'proxy_address','underlying_token_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}
-- OVM1 Launch
{% set project_start_date = '2021-06-23' %}
{% set answer_updated = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f' %}


SELECT
    'optimism' as blockchain
    , c.block_time
    , c.block_date
    , c.block_month
    , c.block_number
    , c.feed_name
    , c.oracle_price
    , c.proxy_address
    , c.aggregator_address
    , COALESCE(o.underlying_token_address, 'n/a') AS underlying_token_address
    , c.oracle_price / POWER(10 , o.extra_decimals) AS underlying_token_price
FROM
(
    SELECT
        l.block_time
        , date_trunc('day', CAST(l.block_time AS timestamp)) as block_date
        , date_trunc('month', CAST(l.block_time AS timestamp)) as block_month
        , l.block_number
	    , cfa.feed_name
	    , AVG(
            conv( --handle for multiple updates in the same block
            substring(l.topic2,3,64)
            ,16,10)
            / POWER(10, cfa.decimals)
            ) AS oracle_price
	    , cfa.proxy_address
        , cfa.aggregator_address
	FROM {{ source('optimism', 'logs') }} l
	INNER JOIN {{ ref('chainlink_optimism_price_feeds_oracle_addresses_legacy') }} cfa
	    ON l.contract_address = cfa.aggregator_address
	WHERE l.topic1 = '{{answer_updated}}'
        {% if not is_incremental() %}
        AND l.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND l.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY
        l.block_time
        , block_date
        , l.block_number
        , cfa.feed_name
        , cfa.proxy_address
        , cfa.aggregator_address
) c
LEFT JOIN {{ ref('chainlink_optimism_price_feeds_oracle_token_mapping_legacy') }} o
	ON c.proxy_address = o.proxy_address
;