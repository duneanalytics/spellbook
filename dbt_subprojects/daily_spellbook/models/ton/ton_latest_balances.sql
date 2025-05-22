{{
    config(
        schema = 'ton',
        alias='latest_balances',
        
        materialized = 'table',
        unique_key = ['address', 'asset'],
        post_hook='{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov"]\') }}'
    )
}}



WITH RANKS AS (
    SELECT address, asset, amount, mintless_claimed, timestamp AS last_change_timestamp, lt AS last_change_lt, 
    row_number() OVER (PARTITION BY address, asset ORDER BY lt DESC) AS rank 
    FROM {{ source('ton', 'balances_history') }}
)
SELECT address, asset, amount, mintless_claimed, last_change_timestamp, last_change_lt
FROM RANKS WHERE rank = 1
