{{
    config(
        alias='KYT',
        post_hook='{{ expose_spells(\'["ethereum", "fantom", "arbitrum", "avalanche_c", "gnosis", "bnb", "optimism", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["whiskey"]\') }}'
    )
}}

-- Find bot traders
--addresses with more than 1000 tx monthly

with initial_bot_list as (
select
distinct "from"
from(
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('ethereum.transactions') }}  group by 1,2 
having count(*) > 1000
union all 
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('polygon.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('optimism.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('arbitrum.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('gnosis.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('fantom.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('bnb.transactions') }}  group by 1,2 
having count(*) > 1000
union all
select "from", date_trunc('month', block_time) as month, count(*) as num_tx from {{ source('avalanche_c.transactions') }} group by 1,2 
having count(*) > 1000
))

 
, final_bot_list as (

select address, 'Bot' as trader_type
from (
select 
cast("from" as varchar) as address 
from initial_bot_list t1
except
(
select address
from {{ ref('labels.all') }}
where category='cex' or name='Ethereum Miner' 
union all
select
cast(address as varchar)
from
{{ source('query_2143144') }}
)
)
)


---Find the Active traders 
,active_traders as (
SELECT 
tx_from,sum(amount_usd) as trade_amount , 
case 
when sum(amount_usd) >=10000 and sum(amount_usd) < 100000 then 'Active Turtle trader'
when sum(amount_usd) >= 100000 and sum(amount_usd) < 500000 then 'Active Shark trader'
when sum(amount_usd) >= 500000 then 'Active Whale trader' end as trader_type
from {{ ref('dex_trades') }}
where block_time > now() - interval '30' day
group by 1
having sum(amount_usd) >10000
)
--Find the Retired traders
,Former_traders as (
SELECT t.tx_from, t.month, t.monthly_trade_amount,
case
when t.monthly_trade_amount >=10000 and t.monthly_trade_amount < 100000 then 'Former Turtle trader'
when t.monthly_trade_amount >= 100000 and t.monthly_trade_amount < 500000 then 'Former Shark trader'
when t.monthly_trade_amount >= 500000 then 'Former Whale trader' end as trader_type
FROM (
    SELECT
        t1.tx_from,
        date_trunc('month', t1.block_time) AS month,
        sum(t1.amount_usd) AS monthly_trade_amount,
        ROW_NUMBER() OVER (PARTITION BY t1.tx_from ORDER BY sum(t1.amount_usd) DESC) AS rn
    FROM {{ ref('dex_trades') }} t1
    left join active_traders t2 on t1.tx_from = t2.tx_from
    where t1.block_time >= now() - interval '1' year and t1.block_time < now() - interval '30' day
    and t2.tx_from is NULL
    GROUP BY 1,2
    HAVING sum(t1.amount_usd) > 10000
) t
WHERE t.rn = 1
ORDER BY t.month, t.monthly_trade_amount DESC )


 



,final as (
SELECT
cast(tx_from as varchar) as address , trader_type
from active_traders
union all
SELECT
cast(tx_from as varchar) as address , trader_type
from Former_traders
union all
SELECT
address , trader_type
from
final_bot_list
)

select 
 "Multi" as blockchain ,
 address ,
 trader_type as name
    , "Dex" as category
    , "whiskey" as contributor
    ,"query"AS source,
    , cast('2023-03-05' as timestamp) as created_at 
    , now() as updated_at
    , "usage" as label_type
    , "KYT.DexGuru" as model_name
from final
;