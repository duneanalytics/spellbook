{{ config(
    
    schema = 'nft_ethereum',
    alias = 'collection_stats',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'nft_contract_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

WITH src_data as
(
    SELECT 
        nft_contract_address
        , block_time
        , CAST(date_trunc('day', block_time) as date) as block_date
        , currency_symbol
        , amount_original
        , amount_usd
    FROM 
        {{ ref('nft_trades') }} 
    WHERE blockchain = 'ethereum'
        AND number_of_items = UINT256 '1'
        AND tx_from != 0x0000000000000000000000000000000000000000
        AND amount_raw > UINT256 '0'
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
),

min_trade_date_per_address as
(
    SELECT 
        MIN(block_date) as first_trade_date, 
        nft_contract_address
    FROM 
        src_data
    GROUP BY
        2 
), 

time_seq AS (
    SELECT 
            {% if is_incremental() %}
                sequence(
                    date_trunc('day', cast(now() as timestamp) - interval '7' day),
                    date_trunc('day', cast(now() as timestamp)),
                    interval '1' day
                ) as time
            {% else %}
                sequence(
                    first_trade_date,
                    date_trunc('day', cast(now() as timestamp)),
                    interval '1' day
                ) as time
            {% endif %}
            , nft_contract_address
    FROM 
        min_trade_date_per_address
),

days AS (
    SELECT 
        time.time AS day,
        nft_contract_address
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),


prices as
(
    SELECT 
        minute
        , price
    FROM 
        {{ source('prices', 'usd') }} prices 
    WHERE
        prices.symbol = 'WETH'
        AND prices.blockchain = 'ethereum'
        AND prices.minute >= TIMESTAMP '2017-06-23' --first trade date
        {% if is_incremental() %}
        AND prices.minute >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
), 

prof_data as
(
    SELECT 
        src.block_date, 
        src.nft_contract_address,
        approx_percentile(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN src.amount_original 
                    ELSE src.amount_usd /prices.price
                END, 
                0.05
        ) as fifth_percentile,
        MIN(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN src.amount_original 
                    ELSE src.amount_usd /prices.price
                END  
        ) as currency_min, 
        MAX(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN src.amount_original 
                    ELSE src.amount_usd /prices.price
                END  
        ) as currency_max, 
        SUM(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN src.amount_original 
                    ELSE src.amount_usd /prices.price
                END  
        ) as currency_volume, 
        COUNT(*) as trades 
    FROM 
        src_data src
    LEFT JOIN
        prices 
        ON prices.minute = date_trunc('minute', src.block_time)
    GROUP BY
        1, 2
)

SELECT 
    CAST(date_trunc('month', d.day) as date) as block_month, 
    d.day as block_date, 
    d.nft_contract_address, 
    COALESCE(prof.currency_volume, 0) as volume_eth,
    COALESCE(prof.trades, 0) as trades, 
    COALESCE(prof.fifth_percentile, 0) as price_p5_eth,
    COALESCE(prof.currency_min, 0) as price_min_eth, 
    COALESCE(prof.currency_max, 0) as price_max_eth
FROM 
    days d
LEFT JOIN 
    prof_data prof 
    ON d.day = prof.block_date
    AND d.nft_contract_address = prof.nft_contract_address