{{ config(
    
    alias = 'prices',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hour', 'blockchain', 'contract_address'],
    post_hook='{{ expose_spells(\'["avalanche_c", "arbitrum", "bnb", "polygon", "ethereum", "gnosis", "optimism", "fantom"]\',
                                "sector",
                                "dex",
                                \'["Henrystats"]\') }}'
    )
}}

WITH

dex_trades as (
    SELECT 
        d.token_bought_address as contract_address, 
        COALESCE(d.amount_usd/d.token_bought_amount, d.amount_usd/(d.token_bought_amount_raw/POW(10, er.decimals))) as price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ source('tokens', 'erc20') }} er
        ON d.token_bought_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND d.token_bought_amount_raw > UINT256 '0'
        {% if is_incremental() %}
        AND d.block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}

    UNION ALL

    SELECT 
        d.token_sold_address as contract_address, 
        COALESCE(d.amount_usd/d.token_sold_amount, d.amount_usd/(d.token_sold_amount_raw/POW(10, er.decimals))) as price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ source('tokens', 'erc20') }} er
        ON d.token_sold_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND d.token_sold_amount_raw > UINT256 '0'
        {% if is_incremental() %}
        AND d.block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
)

SELECT 
    CAST(date_trunc('month', hour) as date) as block_month, -- for partitioning 
    * 
FROM 
(
    SELECT 
        date_trunc('hour', block_time) as hour, 
        contract_address,
        blockchain,
        approx_percentile(price, 0.5) AS median_price,
        COUNT(price) as sample_size 
    FROM dex_trades
    GROUP BY 1, 2, 3
    HAVING COUNT(price) >= 5 
) tmp