{{ config(
        schema = 'metrics_bitcoin'
        , alias = 'transfers_daily_address'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


with raw_transfers as (
    select
        'bitcoin' as blockchain
        , block_date
        , wallet_address as address
        , 'sent' as transfer_direction
        , (sum(abs(amount_transfer_usd)) * -1) as transfer_amount_usd
    from
        {{ source('transfers_bitcoin', 'satoshi') }}
    where
        1 = 1
        and type = 'input'
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , wallet_address
        , 'sent'

    union all

    select
        'bitcoin' as blockchain
        , block_date
        , wallet_address as address
        , 'received' as transfer_direction
        , sum(abs(amount_transfer_usd)) as transfer_amount_usd
    from
        {{ source('transfers_bitcoin', 'satoshi') }}
    where
        1 = 1
        and type = 'output'
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , wallet_address
        , 'received'
), labels as (
    select
        od.owner_key
        , od.name
        , od.primary_category
        , od.country_name
        , oa.blockchain
        , oa.address
    from
        {{ source('labels', 'owner_addresses') }} as oa
    inner join
        {{ source('labels', 'owner_details') }} as od
        on oa.owner_key = od.owner_key
    where
        oa.blockchain = 'bitcoin'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.address
        , coalesce(l.name, 'Unknown') as name
        , coalesce(l.primary_category, 'Uncategorized') as primary_category
        , coalesce(l.country_name, 'Unknown') as hq_country
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
    from
        raw_transfers as t
    left join
        labels as l
        on t.blockchain = l.blockchain
        and cast(t.address as varbinary) = l.address --labels pipeline uses this cast approach, match here in join condition to be safe
    where
        coalesce(l.primary_category, 'n/a') not in ('Hacks and exploits', 'Social Engineering Scams') -- filter out scam addresses
    group by
        t.blockchain
        , t.block_date
        , t.address
        , coalesce(l.name, 'Unknown')
        , coalesce(l.primary_category, 'Uncategorized')
        , coalesce(l.country_name, 'Unknown')
), net_transfers as (
    select
        blockchain
        , block_date
        , address
        , name
        , primary_category
        , hq_country
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , address
        , name
        , primary_category
        , hq_country
)
select
    blockchain
    , block_date
    , case
        when substring(address, 1, 3) = 'bc1' then cast(address as varbinary) --we don't have bech32() function for this address type
        else from_base58(address) --all other address types *should* be fine to use base58
    end as address
    , name
    , primary_category
    , hq_country
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(abs(transfer_amount_usd_sent)) + sum(abs(transfer_amount_usd_received)) as transfer_amount_usd
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date
    , address
    , name
    , primary_category
    , hq_country
