{% set blockchain = 'bitcoin' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with prices as (
        select
            day
            , price
        from
            {{ source('prices', 'usd_daily') }}
        where
            symbol = 'BTC'
            and blockchain is null
            {% if is_incremental() or true %}
            and {{ incremental_predicate('day') }}
            {% endif %}
)
, bitcoin_fees as (
        select
            block_date
            , case
                when substring(input[1][6][1], 1, 3) = 'bc1' then cast(input[1][6][1] as varbinary) --we don't have bech32() function for this address type
                else from_base58(input[1][6][1]) --all other address types *should* be fine to use base58
            end as address
            , sum(fee) as daily_fee
        from
            {{ source(blockchain, 'transactions') }}
        where
            input[1][6][1] is not null --first transaction of the block is the mined value and has no address
            and block_date < cast(date_trunc('day', now()) as date) --exclude current day to match prices.usd_daily
            {% if is_incremental() or true %}
            and {{ incremental_predicate('block_date') }}
            {% endif %}
        group by
            1
            , 2
)
select
    '{{ blockchain }}' as blockchain
    , fees.block_date
    , fees.address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , (fees.daily_fee * prices.price) as gas_fees_usd
from
    bitcoin_fees as fees
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on oa.blockchain = '{{ blockchain }}'
    and fees.address = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key
inner join prices
    on fees.block_date = prices.day