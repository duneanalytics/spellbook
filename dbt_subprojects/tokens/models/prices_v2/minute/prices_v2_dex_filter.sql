{{ config(
    schema='prices_v2'
    , alias = 'dex_filter'
    , materialized = 'view'
    )
}}

WITH dex_volume_over_10k as (
    select
        blockchain
        ,contract_address
    from(
        SELECT
            d.blockchain,
            d.token_bought_address as contract_address,
            sum(d.amount_usd) as volume -- in USD
        FROM {{ source('dex','trades') }} d
        group by 1,2
        UNION ALL
        SELECT
            d.blockchain,
            d.token_sold_address as contract_address,
            sum(d.amount_usd) as volume -- in USD
        FROM {{ source('dex','trades') }} d
        group by 1,2
    )
    group by 1,2
    having sum(volume) >= 10000
)

select *
from dex_volume_over_10k
