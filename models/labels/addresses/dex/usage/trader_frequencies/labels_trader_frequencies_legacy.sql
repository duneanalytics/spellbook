{{
    config(
	tags=['legacy'],
	
        alias = alias('trader_frequencies', legacy_model=True),
        post_hook='{{ expose_spells(\'
        ["ethereum", "fantom", "arbitrum", "avalanche_c", "gnosis", "bnb", "optimism", "polygon"]\', 
        "sector", 
        "labels", 
        \'["gentrexha", "Henrystats"]\') }}'
    )
}}

with
 trader_frequencies as (
    select
        blockchain,
        taker as address,
        count(distinct tx_hash) / datediff(max(block_date), min(block_date)) as trades_per_day
    from (
        select blockchain, taker, block_date, tx_hash
        from {{ ref('dex_aggregator_trades_legacy') }}
        UNION ALL
        select blockchain, taker, block_date, tx_hash
        from {{ ref('dex_trades_legacy') }}
    )
    group by taker, blockchain
    -- That have at least more than 1 trade
    having datediff(max(block_date), min(block_date)) > 0
 )

select blockchain       AS blockchain,
       address,
       case
           when trades_per_day >= 1 then 'Daily Trader'
           when trades_per_day >= 0.142857142857 then 'Weekly Trader'
           when trades_per_day >= 0.0333333333333 then 'Monthly Trader'
           when trades_per_day >= 0.0027397260274 then 'Yearly Trader'
           else 'Sparse Trader'
           end                 AS name,
       "dex"    AS category,
       "gentrexha"             AS contributor,
       "query"                 AS source,
       timestamp('2022-12-14') AS created_at,
       now()                   AS updated_at,
        "trader_frequencies" as model_name,
        "usage" as label_type
from trader_frequencies
;
