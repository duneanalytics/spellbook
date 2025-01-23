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
, manual_filter as (
    SELECT
        blockchain,
        contract_address
    FROM (
        VALUES
            ('ethereum', 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3)    -- USDe has bad events (ex https://etherscan.io/tx/0x0c9464ff4fea893667a43e96e830073031f5587d8f3b33fb27a8464979f12897#eventlog#151)
    ) as t(blockchain, contract_address)
)

select *
from dex_volume_over_10k
where (blockchain, contract_address)
    not in (select blockchain, contract_address from manual_filter)
