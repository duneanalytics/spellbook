{{ config(
        schema = 'metrics_bitcoin'
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
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
            {% if is_incremental() %}
            and {{ incremental_predicate('day') }}
            {% endif %}
)
, bitcoin_fees as (
        select
            date as block_date
            , sum(total_fees) as daily_fee
        from
            {{ source('bitcoin', 'blocks') }}
        where
            date < cast(date_trunc('day', now()) as date) --exclude current day to match prices.usd_daily
            {% if is_incremental() %}
            and {{ incremental_predicate('date') }}
            {% endif %}
        group by
            date
)
select
    'bitcoin' as blockchain
    , btc.block_date
    , (btc.daily_fee * prices.price) as gas_fees_usd
from
    btc
inner join prices
    on btc.block_date = prices.day
