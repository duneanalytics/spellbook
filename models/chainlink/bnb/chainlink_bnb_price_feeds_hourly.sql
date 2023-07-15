{{ config(
    alias = alias('price_feeds_hourly'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'hour', 'proxy_address', 'underlying_token_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll"]\') }}'
    )
}}

{% set project_start_date = '2020-08-29' %}

WITH gs AS (
    SELECT
        oa.hr,
        oa.feed_name,
        oa.proxy_address,
        oa.aggregator_address,
        c.underlying_token_address
    FROM (
        SELECT explode(
                sequence(
                        {% if not is_incremental() %}
                            DATE_TRUNC('hour', cast('{{project_start_date}}' as date)),
                        {% endif %}
                        {% if is_incremental() %}
                            DATE_TRUNC('hour', now() - interval '1 week'),
                        {% endif %}
                        DATE_TRUNC('hour', NOW()),
                        interval '1 hour'
                        )
            ) AS hr, 
                feed_name,
                proxy_address,
                aggregator_address
        FROM {{ ref('chainlink_bnb_oracle_addresses') }}
    ) oa LEFT JOIN {{ ref('chainlink_bnb_oracle_token_mapping') }} c ON c.proxy_address = oa.proxy_address
)

SELECT 'bnb'                                                AS blockchain,
        hour,
        DATE_TRUNC('day',hour)                              AS block_date,
        feed_name,
        proxy_address,
        aggregator_address,
        underlying_token_address,
        oracle_price_avg,
        underlying_token_price_avg
FROM (
    SELECT
        hr                                                  AS hour,
        feed_name,
        proxy_address,
        aggregator_address,
        underlying_token_address,
        first_value(oracle_price_avg) 
            OVER (
                PARTITION BY feed_name, 
                             proxy_address,
                             aggregator_address,
                             underlying_token_address,
                             grp 
                ORDER BY hr)                                AS oracle_price_avg,
        first_value(underlying_token_price_avg) 
            OVER (
                PARTITION BY feed_name,
                             proxy_address,
                             aggregator_address,
                             underlying_token_address,
                             grp
                ORDER BY hr)                                AS underlying_token_price_avg
    FROM
    (
        SELECT
            hr,
            feed_name,
            proxy_address,
            aggregator_address,
            oracle_price_avg,
            underlying_token_address,
            underlying_token_price_avg,
            count(oracle_price_avg) 
                OVER (
                    PARTITION BY feed_name,
                                 proxy_address,
                                 aggregator_address,
                                 underlying_token_address
                ORDER BY hr)                                AS grp
        FROM (
            SELECT gs.hr,
                gs.feed_name,
                gs.proxy_address,
                gs.aggregator_address,
                AVG(oracle_price)                           AS oracle_price_avg,
                gs.underlying_token_address,
                AVG(underlying_token_price)                 AS underlying_token_price_avg
            FROM gs LEFT JOIN {{ ref('chainlink_bnb_price_feeds') }} f
                 ON gs.hr = DATE_TRUNC('day',f.block_time)
                 AND gs.underlying_token_address = f.underlying_token_address
                 AND gs.proxy_address = f.proxy_address
                 AND gs.aggregator_address = f.aggregator_address
            WHERE
                {% if not is_incremental() %}
                gs.hr >= '{{project_start_date}}'
                {% endif %}
                {% if is_incremental() %}
                gs.hr >= date_trunc('hour', now() - interval '1 week')
                {% endif %}
            GROUP BY
                gs.hr,
                gs.feed_name,
                gs.proxy_address,
                gs.aggregator_address,
                gs.underlying_token_address
        ) a
    ) b
) c
WHERE oracle_price_avg IS NOT NULL --don't overwrite where we don't have a value