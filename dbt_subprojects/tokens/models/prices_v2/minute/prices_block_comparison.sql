{{ config(
    schema='prices_v2'
    , alias = 'comparison'
    , materialized = 'tabe'
    , file_format = 'delta'
    )
}}

WITH prices_block_vwap as(
SELECT
    b.blockchain,
    b.contract_address,
    date_trunc('minute',block_time) as timestamp,
    sum(amount_usd*b.price)/sum(amount_usd) as price  --vwap
FROM {{source('dex','prices_block')}} b
inner join {{ref('prices_v2_dex_minute_raw')}} m
    on b.blockchain= m.blockchain
    and b.contract_address = m.contract_address
    and date_trunc('minute',block_time) = m.timestamp
group by 1,2,3
)

select
    blockchain
    ,contract_address
    ,timestamp
    ,b.price as block_based_price
    ,m.price as minute_raw_price
    ,(m.price - b.price) as absolute_difference
    ,(m.price - b.price)/b.price as percent_difference
    ,(m.vwap_price - b.price) as absolute_difference_vwap
    ,(m.vwap_price - b.price)/b.price as percent_difference_vwap
from prices_block_vwap b
full outer join {{ref('prices_v2_dex_minute_raw')}} m using (blockchain,contract_address,timestamp)
