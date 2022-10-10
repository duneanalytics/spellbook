{{ config(
    schema = 'chainlink_optimism',
    alias = 'price_feeds',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'proxy_address', 'aggregator_address', 'underlying_token_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["msilb7"]\') }}'
    )
}}
-- OVM1 Launch
{% set project_start_date = '2021-06-23' %}


SELECT
'optimism' as blockchain, block_time, block_date, c.feed_name, c.oracle_price, c.proxy_address, c.aggregator_address, underlying_token_address
, c.price / POWER(10 , extra_decimals) AS underlying_token_price
FROM (
	SELECT
	block_time
    , DATE_TRUNC('day',block_time) AS block_date
	, feed_name
	, conv(topic2,16,10) / POWER(10,decimals) AS oracle_price
	,`proxy_address`, `aggregator_address`
	FROM {{ source('optimism', 'logs') }} l
	INNER JOIN {{ ref('chainlink_optimism_oracle_addresses') }} cfa
	    ON l.contract_address = cfa.address
	WHERE topic1 = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f' --Answer Updated
	{% if not is_incremental() %}
    AND l.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND l.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

	) c
LEFT JOIN chainlink.oracle_token_mapping o
	ON c.proxy = o.proxy
