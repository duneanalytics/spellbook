{{ config(
    alias = alias('collection_stats'),
    partition_by = ['block_date'],
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
        , date_trunc('day', block_time) as block_date
        , currency_symbol
        , amount_original
        , amount_usd
    FROM 
        {{ ref('nft_trades') }} 
    WHERE blockchain = 'ethereum'
        AND number_of_items = 1
        AND tx_from != LOWER('0x0000000000000000000000000000000000000000')
        AND amount_raw > 0
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
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

days as
(
    SELECT
        {% if is_incremental() %}
        explode(
            sequence(
                date_trunc("day", now() - interval '1 week'), date_trunc('day', now()), interval 1 day
            )
        ) as day
        {% else %}
        explode(
            sequence(
                to_date(first_trade_date), date_trunc('day', now()), interval 1 day -- first trade date in nft.trades
            )
        ) as day
        {% endif %}
        , nft_contract_address
    FROM
        min_trade_date_per_address
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
        AND prices.minute >= '2017-06-23' --first trade date
        {% if is_incremental() %}
        AND prices.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

prof_data as
(
    SELECT 
        src.block_date, 
        src.nft_contract_address,
        percentile_cont(.05) WITHIN GROUP 
            (ORDER BY 
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN src.amount_original 
                    ELSE src.amount_usd /prices.price
                END       
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