{{ config(
    tags=['dunesql'],
    alias = alias('balances'),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}

WITH balances as (
    SELECT block_date as day,
           SUM(value) as value,
           dao,
           dao_wallet_address,
           dao_creator_tool,
           blockchain,
           asset_contract_address,
           asset
    FROM
        {{ ref('dao_transactions') }}
    WHERE tx_type = 'tx_in'
    GROUP BY 1, 3, 4, 5, 6, 7, 8

    UNION ALL

    SELECT
        block_date as day,
        -1 * SUM(value) as value,
        dao,
        dao_wallet_address,
        dao_creator_tool,
        blockchain,
        asset_contract_address,
        asset
    FROM
        {{ ref('dao_transactions') }}
    WHERE tx_type = 'tx_out'
    GROUP BY 1, 3, 4, 5, 6, 7, 8
),

balances_all as (
    SELECT day,
           SUM(value) as value,
           dao,
           dao_wallet_address,
           dao_creator_tool,
           blockchain,
           asset_contract_address,
           asset
    FROM balances
    GROUP BY 1, 3, 4, 5, 6, 7, 8
),


time_seq AS (
    SELECT 
        sequence(
        CAST('2018-10-27' as timestamp),
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

daily_balances as (
    SELECT *,
           SUM(value)
               OVER (PARTITION BY dao, dao_wallet_address, asset_contract_address, blockchain ORDER BY day)  as balance,
           lead(day, 1, now())
                OVER (PARTITION BY dao, dao_wallet_address, asset_contract_address, blockchain ORDER BY day) AS next_day
    FROM balances_all
)

SELECT d.day,
       db.blockchain,
       db.dao_creator_tool,
       db.dao,
       db.dao_wallet_address,
       db.balance,
       db.balance * COALESCE(p.price, e.price) as usd_value,
       db.asset,
       db.asset_contract_address
FROM daily_balances db
INNER JOIN days d
    ON db.day <= d.day
    AND d.day < db.next_day
LEFT JOIN
   {{ source('prices', 'usd') }} p
    ON p.contract_address = db.asset_contract_address
    AND d.day = p.minute
    AND p.blockchain = db.blockchain
LEFT JOIN 
    {{ source('prices', 'usd') }} e 
    ON db.asset_contract_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    AND d.day = e.minute
    AND db.blockchain = 'ethereum'
    AND e.blockchain = 'ethereum'
    AND e.symbol = 'WETH'