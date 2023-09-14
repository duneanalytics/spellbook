{{ config(
        alias = alias('prices_latest'),
        tags = ['dunesql'],
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_time', 'token_address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["bernat"]\') }}'
        )
}}

SELECT
    block_month,
    block_time,
    token_address,
    token_price_usd,
    token_price_usd_raw
FROM (
    SELECT
        block_month,
        block_time,
        token_address,
        amount_usd / token_amount AS token_price_usd,
        amount_usd / token_amount_raw AS token_price_usd_raw,
        amount_usd,
        token_amount,
        token_amount_raw,
        ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY block_time DESC) AS rn
    FROM (
        SELECT
            CAST(date_trunc('month', block_time) as DATE) as block_month,
            block_time,
            token_bought_address AS token_address,
            token_bought_amount AS token_amount,
            token_bought_amount_raw AS token_amount_raw,
            amount_usd
        FROM {{ ref('dex_trades') }}
        WHERE
            amount_usd > 0
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        UNION ALL
        SELECT
            CAST(date_trunc('month', block_time) as DATE) as block_month,
            block_time,
            token_sold_address AS token_address,
            token_sold_amount AS token_amount,
            token_sold_amount_raw AS token_amount_raw,
            amount_usd
        FROM {{ ref('dex_trades') }}
        WHERE
            amount_usd > 0
            {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    ) subquery
) most_recent
WHERE rn = 1