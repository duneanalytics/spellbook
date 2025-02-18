{% macro evm_net_transfers_daily(blockchain) %} 

with raw_transfers as (
    select
        blockchain
        , block_date
        , "from" as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
    from
        {{ ref('tokens_transfers') }}
    where
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , "from"
        , 'sent'

    union all

    select
        blockchain
        , block_date
        , to as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
    from
        {{ ref('tokens_transfers') }}
    where
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , to
        , 'received'
), labels as (
    select
        od.owner_key
        , od.primary_category
        , oa.blockchain
        , oa.address
    from
        {{ source('labels', 'owner_addresses') }} as oa
    inner join
        {{ source('labels', 'owner_details') }} as od
        on oa.owner_key = od.owner_key
    where oa.blockchain = '{{blockchain}}'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , coalesce(l.owner_key, cast(t.address as varchar)) as address_owner
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
    from
        raw_transfers as t
    left join
        labels as l
        on t.blockchain = l.blockchain
        and t.address = l.address
    where
        coalesce(l.primary_category, 'n/a') not in ('Hacks and exploits', 'Social Engineering Scams') -- filter out scam addresses
    group by
        t.blockchain
        , t.block_date
        , coalesce(l.owner_key, cast(t.address as varchar))
), net_transfers as (
    select
        blockchain
        , block_date
        , address_owner
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , address_owner
)
select
    blockchain
    , block_date
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

{% endmacro %}


{% ############################################################################################ %}

{% macro evm_net_transfers_daily_asset(blockchain, native_contract_address) %}
with raw_transfers as (
    select
        blockchain
        , block_date
        , COALESCE(contract_address, {{ native_contract_address }}) as contract_address
        , symbol
        , "from" as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
        , count(*) transfer_count
    from
        {{ ref('tokens_transfers') }}
    where
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , COALESCE(contract_address, {{ native_contract_address }}) 
        , symbol
        , "from"
        , 'sent'

    union all

    select
        blockchain
        , block_date
        , COALESCE(contract_address, {{ native_contract_address }}) as contract_address
        , symbol
        , to as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
        , count(*) transfer_count
    from
        {{ ref('tokens_transfers') }}
    where
        blockchain = '{{blockchain}}'
       {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , COALESCE(contract_address, {{ native_contract_address }})
        , symbol
        , to
        , 'received'
),  transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
        , sum(transfer_count) as transfer_count
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
), net_transfers as (
    select
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
        , sum(transfer_count) transfer_count
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
)
select
    blockchain
    , block_date
    , contract_address
    , symbol
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(abs(transfer_amount_usd_sent)) + sum(abs(transfer_amount_usd_received)) as transfer_amount_usd
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
    , sum(transfer_count) transfer_count
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date
    , contract_address
    , symbol

{% endmacro %}