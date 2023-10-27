{{ config(
    
    alias = 'balances_steth',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}

WITH 

time_seq AS (
    SELECT 
        sequence(
        CAST('2020-12-22' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) AS time 
),

days AS (
    SELECT 
        time.time AS day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

wsteth_volume as (
    SELECT 
        CAST(date_trunc('day', call_block_time) as date) as day,
        call_block_time,
        output_0/1e18 as steth,
        _wstETHAmount/1e18 as wsteth 
    FROM  
    {{ source('lido_ethereum', 'WstETH_call_unwrap') }}
    WHERE call_success = true 
    
    UNION ALL 

    SELECT
        CAST(date_trunc('day', call_block_time) as date) as day,
        call_block_time, 
        _stETHAmount/1e18 as steth, 
        output_0/1e18 as wsteth 
    FROM  
    {{ source('lido_ethereum', 'WstETH_call_wrap') }}
    WHERE call_success = true
),

wsteth_rate as (
    SELECT 
        CAST(date_trunc('day', call_block_time) as date) as day, 
        CASE 
            WHEN CAST(extract(hour from call_block_time) as double) >= 12 AND CAST(extract(hour from call_block_time) as double) <= 23 
            THEN 'post_rebase'
            ELSE 'pre_rebase'
        END as rebase_key,
        CASE 
            WHEN CAST(extract(hour from call_block_time) as double) >= 12 AND CAST(extract(hour from call_block_time) as double) <= 23 
            THEN 1
            ELSE 2
        END as rebase_rank_key,
        SUM(CAST(steth as double))/SUM(CAST(wsteth as double)) as rate 
    FROM 
    wsteth_volume
    GROUP BY 1, 2, 3 
), 

transactions as (
    SELECT  
        block_date as day, 
        CASE 
            WHEN CAST(extract(hour from block_time) as double) >= 12 AND CAST(extract(hour from block_time) as double) <= 23 
            THEN 'post_rebase'
            ELSE 'pre_rebase'
        END as rebase_key,
        SUM(CASE 
            WHEN tx_type = 'tx_in' THEN value ELSE -value END) as steth_transfers, 
        dao_wallet_address
    FROM 
    {{ ref('dao_transactions') }}
    WHERE block_date >= CAST('2020-12-22' as timestamp)
    AND blockchain = 'ethereum'
    AND asset_contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
    GROUP BY 1, 2, 4 
),

transactions_enriched as (
    SELECT 
        t.day, 
        t.steth_transfers,
        SUM(t.steth_transfers/COALESCE(w.rate, 1)) as wsteth_balance, 
        t.dao_wallet_address
    FROM 
    transactions t 
    LEFT JOIN 
    wsteth_rate w 
        ON t.day = w.day 
        AND t.rebase_key = w.rebase_key
    GROUP BY 1, 2, 4 
), 

balances_daily as (
    SELECT 
        day, 
        SUM(steth_transfers) OVER (PARTITION BY dao_wallet_address ORDER BY day) as steth_cum, 
        SUM(wsteth_balance) OVER (PARTITION BY dao_wallet_address ORDER BY day) as wsteth_cum, 
        lead(day, 1, now())
                OVER (PARTITION BY dao_wallet_address ORDER BY day) AS next_day,
        dao_wallet_address
        FROM 
        transactions_enriched
),

balances_enriched as (
    SELECT 
        d.day, 
        b.steth_cum, 
        b.wsteth_cum, 
        b.dao_wallet_address
    FROM 
    balances_daily b
    INNER JOIN 
    days d
        ON b.day <= d.day
        AND d.day < b.next_day
),

rates_ranked as (
    SELECT 
        day, 
        MIN_BY(rate, rebase_rank_key) as rate 
    FROM 
    wsteth_rate 
    GROUP BY 1 
),

balances_before_final as (
    SELECT 
        b.day, 
        b.dao_wallet_address, 
        COALESCE(b.wsteth_cum * r.rate, b.steth_cum) as balance_final 
    FROM 
    balances_enriched b 
    LEFT JOIN 
    rates_ranked r 
        ON b.day = r.day 
)

SELECT 
    b.day, 
    'ethereum' as blockchain, 
    da.dao_creator_tool,
    da.dao, 
    b.dao_wallet_address,
    b.balance_final as balance, 
    b.balance_final * p.price as usd_value,
    'stETH' as asset, 
    0xae7ab96520de3a18e5e111b5eaab095312d7fe84 as asset_contract_address
FROM 
balances_before_final b 
INNER JOIN 
{{ ref('dao_addresses_ethereum') }} da 
    ON b.dao_wallet_address = da.dao_wallet_address
LEFT JOIN
   {{ source('prices', 'usd') }} p
    ON p.contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
    AND b.day = p.minute
    AND p.blockchain = 'ethereum'


