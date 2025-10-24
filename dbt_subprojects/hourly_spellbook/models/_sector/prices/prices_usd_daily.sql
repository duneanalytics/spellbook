{{ config(
        schema='prices',
        alias = 'usd_daily',
        materialized = 'table',
        file_format = 'delta',
        post_hook = '{{ hide_spells() }}'
        )
}}

select * from (
select
    cast(date_trunc('day', minute) as date) as day,
    blockchain,
    contract_address,
    decimals,
    symbol,
    avg(price) as price,
    min_by(price,minute) as price_open,
    max(price) as price_high,
    min(price) as price_low,
    max_by(price,minute) as price_close
from {{ source('prices', 'usd') }}
group by 1,2,3,4,5
)
where day < cast(date_trunc('day',now()) as date) -- exclude ongoing day

