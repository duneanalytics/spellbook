{{ config(
    tags=['dunesql'],
    alias = alias('prices'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hour', 'blockchain', 'contract_address'],
    post_hook='{{ expose_spells(\'["avalanche_c", "arbitrum", "bnb", "polygon", "ethereum", "gnosis", "optimism", "fantom"]\',
                                "sector",
                                "dex",
                                \'["Henrystats", "m_r_g_t"]\') }}'
    )
}}

WITH 
dex_trades AS (
    SELECT 
        d.token_bought_address AS contract_address, 
        COALESCE(d.amount_usd/d.token_bought_amount, d.amount_usd/(cast(d.token_bought_amount_raw as double)/POW(10, er.decimals))) AS price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ ref('tokens_erc20') }} er
        ON d.token_bought_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND cast(d.token_bought_amount_raw as double) > 0
        {% if is_incremental() %}
        AND d.block_time >= date_add('week', -1, current_date)
        {% endif %}

    UNION ALL

    SELECT 
        d.token_sold_address AS contract_address, 
        COALESCE(d.amount_usd/d.token_sold_amount, d.amount_usd/(cast(d.token_sold_amount_raw as double)/POW(10, er.decimals))) AS price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ ref('tokens_erc20') }} er
        ON d.token_sold_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND cast(d.token_bought_amount_raw as double) > 0
        {% if is_incremental() %}
        AND d.block_time >= date_add('week', -1, current_date)
        {% endif %}
)

SELECT 
    TRY_CAST(date_trunc('day', hour) AS date) AS day, -- for partitioning 
    * 
FROM 
(
    SELECT 
        date_trunc('hour', block_time) AS hour, 
        contract_address,
        blockchain,
        approx_percentile(price, 0.5) AS median_price,
        COUNT(price) AS sample_size 
    FROM dex_trades
    GROUP BY 1, 2, 3
    HAVING COUNT(price) >= 5 
) tmp
;