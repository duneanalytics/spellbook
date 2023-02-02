{{ config(
    alias = 'floor_price',
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

WITH 

src_data as (
    SELECT 
        *,
        date_trunc('day', block_time) as block_date
    FROM 
    {{ ref('nft_trades') }} 
    WHERE blockchain = 'ethereum'
    AND trade_type = 'Single Item Trade'
    AND tx_from != LOWER('0x0000000000000000000000000000000000000000')
    AND amount_raw > 0 
    {% if is_incremental() %}
    AND block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

min_trade_date as (
    SELECT 
        MIN(block_date) as first_trade_date, 
        nft_contract_address
    FROM 
    src_data
    GROUP BY 2 
), 

days as (
    SELECT 
        explode(
            sequence(
                to_date(first_trade_date), date_trunc('day', now()), interval 1 day -- first trade date in nft.trades
            )
        ) as day,
        nft_contract_address
    FROM 
    min_trade_date
), 

prices as (
    SELECT 
        *
    FROM 
    {{ source('prices', 'usd') }} prices 
    WHERE prices.symbol = 'WETH'
    AND prices.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND prices.minute >= '2017-06-23'
    {% endif %}
    {% if is_incremental() %}
    AND prices.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
), 

prof_data as (
    SELECT 
        src.block_date, 
        src.nft_contract_address,
        percentile_cont(.05) WITHIN GROUP 
            (ORDER BY src.amount_original/(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN 1 
                    ELSE prices.price
                END
            )         
        ) as fifth_percentile, 
        MIN(
            src.amount_original/(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN 1 
                    ELSE prices.price
                END 
            )
        ) as currency_min, 
        MAX(
            src.amount_original/(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN 1 
                    ELSE prices.price
                END 
            )
        ) as currency_max, 
        SUM(
            src.amount_original/(
                CASE 
                    WHEN src.currency_symbol IN ('ETH', 'WETH') THEN 1 
                    ELSE prices.price
                END 
            )
        ) as currency_volume, 
        COUNT(*) as trades 
    FROM 
    src_data src 
    LEFT JOIN 
    prices 
        ON prices.minute = date_trunc('minute', src.block_time)
    GROUP BY 1, 2
)

SELECT 
    d.day as block_date, 
    d.nft_contract_address, 
    COALESCE(prof.currency_volume, 0) as currency_volume,
    COALESCE(prof.trades, 0) as trades, 
    COALESCE(prof.fifth_percentile, 0) as fifth_percentile,
    COALESCE(prof.currency_min, 0) as currency_min, 
    COALESCE(prof.currency_max, 0) as currency_max 
FROM 
days d 
LEFT JOIN 
prof_data prof 
    ON d.day = prof.block_date
    AND d.nft_contract_address = prof.nft_contract_address