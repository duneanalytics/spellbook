{{ config(
        alias = alias('prices_latest'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'token_address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["bernat"]\') }}'
        )
}}

SELECT
    block_time,
    token_address,
    token_price_usd,
    token_price_usd_raw
FROM (
    SELECT
        block_time,
        token_address,
        amount_usd / token_amount AS token_price_usd,
        amount_usd / cast(token_amount_raw as uint256) AS token_price_usd_raw,
        amount_usd,
        token_amount,
        token_amount_raw,
        ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY block_time DESC) AS rn
    FROM (
        SELECT
            block_time,
            token_bought_address AS token_address,
            token_bought_amount AS token_amount,
            token_bought_amount_raw AS token_amount_raw,
            amount_usd
        FROM dex.trades
        {% if is_incremental() %}
        WHERE block_time >= now() - interval '7' day
        {% endif %}
        UNION ALL
        SELECT
            block_time,
            token_sold_address AS token_address,
            token_sold_amount AS token_amount,
            token_sold_amount_raw AS token_amount_raw,
            amount_usd
        FROM dex.trades
        {% if is_incremental() %}
        WHERE block_time >= now() - interval '7' day
        {% endif %}
    ) subquery
) most_recent
WHERE rn = 1 
AND amount_usd > 0
