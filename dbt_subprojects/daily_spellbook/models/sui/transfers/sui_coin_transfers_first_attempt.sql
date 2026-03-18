{{ config(
    schema = 'sui_transfers'
    , alias = 'coin_transfers_first_attempt'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_key']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , tags = ['sui', 'tokens', 'transfers']
) }}

{% set sui_transfer_start_date = var('sui_transfer_start_date', '2026-01-01') %}
{% set sui_transfer_coin_type = '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC' %}

with day_rows as (
    select
        o.object_id
        , o.version
        , o.previous_transaction as tx_digest
        , o.timestamp_ms
        , o.date as block_date
        , cast(date_trunc('month', o.date) as date) as block_month
        , o.checkpoint
        , o.owner_type
        , o.owner_address as receiver
        , o.coin_type
        , o.object_status
        , try_cast(o.coin_balance as bigint) as coin_balance
    from {{ source('sui', 'objects') }} o
    where o.object_status in ('Created', 'Mutated')
        and o.coin_type is not null
        and o.coin_type = '{{ sui_transfer_coin_type }}'
        and o.date >= date '{{ sui_transfer_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('o.date') }}
        {% endif %}
)
, pre_range_usdc_ids as (
    select distinct p.object_id
    from {{ source('sui', 'objects') }} p
    where p.coin_type = '{{ sui_transfer_coin_type }}'
        and p.object_status in ('Created', 'Mutated')
        and p.date < date '{{ sui_transfer_start_date }}'
)
, deleted_rows as (
    select
        o.object_id
        , o.version
        , o.previous_transaction as tx_digest
        , o.timestamp_ms
        , o.date as block_date
        , cast(date_trunc('month', o.date) as date) as block_month
        , o.checkpoint
        , cast(null as varchar) as owner_type
        , cast(null as varbinary) as receiver
        , cast(null as varchar) as coin_type
        , o.object_status
        , cast(0 as bigint) as coin_balance
    from {{ source('sui', 'objects') }} o
    where o.object_status = 'Deleted'
        and o.date >= date '{{ sui_transfer_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('o.date') }}
        {% endif %}
        and o.object_id in (
            select d.object_id from day_rows d
            union
            select p.object_id from pre_range_usdc_ids p
        )
)
, anchors as (
    select
        p.object_id
        , max(p.version) as version
        , cast(null as varchar) as tx_digest
        , max_by(p.timestamp_ms, p.version) as timestamp_ms
        , cast(date '{{ sui_transfer_start_date }}' as date) as block_date
        , cast(date_trunc('month', date '{{ sui_transfer_start_date }}') as date) as block_month
        , max_by(p.checkpoint, p.version) as checkpoint
        , max_by(p.owner_type, p.version) as owner_type
        , max_by(p.owner_address, p.version) as receiver
        , p.coin_type
        , cast('ANCHOR' as varchar) as object_status
        , max_by(try_cast(p.coin_balance as bigint), p.version) as coin_balance
    from {{ source('sui', 'objects') }} p
    where p.object_status in ('Created', 'Mutated')
        and p.coin_type is not null
        and p.coin_type = '{{ sui_transfer_coin_type }}'
        and p.date < date '{{ sui_transfer_start_date }}'
        and p.object_id in (
            select distinct d.object_id from day_rows d
            union
            select distinct dr.object_id from deleted_rows dr
        )
    group by p.object_id, p.coin_type
)
, unioned as (
    select
        a.object_id
        , a.version
        , a.tx_digest
        , a.timestamp_ms
        , a.block_date
        , a.block_month
        , a.checkpoint
        , a.owner_type
        , a.receiver
        , a.coin_type
        , a.object_status
        , a.coin_balance
    from anchors a

    union all

    select
        d.object_id
        , d.version
        , d.tx_digest
        , d.timestamp_ms
        , d.block_date
        , d.block_month
        , d.checkpoint
        , d.owner_type
        , d.receiver
        , d.coin_type
        , d.object_status
        , d.coin_balance
    from day_rows d

    union all

    select
        dr.object_id
        , dr.version
        , dr.tx_digest
        , dr.timestamp_ms
        , dr.block_date
        , dr.block_month
        , dr.checkpoint
        , dr.owner_type
        , dr.receiver
        , dr.coin_type
        , dr.object_status
        , dr.coin_balance
    from deleted_rows dr
)
, calc as (
    select
        u.object_id
        , u.version
        , u.tx_digest
        , u.timestamp_ms
        , u.block_date
        , u.block_month
        , u.checkpoint
        , coalesce(u.owner_type, lag(u.owner_type) over w) as owner_type
        , coalesce(u.receiver, lag(u.receiver) over w) as receiver
        , coalesce(u.coin_type, lag(u.coin_type) over w) as coin_type
        , u.object_status
        , u.coin_balance
        , lag(u.receiver) over w as prev_owner
        , lag(u.coin_balance) over w as prev_balance
    from unioned u
    window w as (partition by u.object_id order by u.version)
)
, tx_senders as (
    select
        t.transaction_digest as tx_digest
        , t.sender as tx_sender
    from {{ source('sui', 'transactions') }} t
    inner join (
        select distinct
            c.tx_digest
        from calc c
        where c.tx_digest is not null
    ) d
        on t.transaction_digest = d.tx_digest
    where t.date >= date '{{ sui_transfer_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('t.date') }}
        {% endif %}
)
, enriched as (
    select
        c.object_id
        , c.version
        , c.tx_digest
        , c.timestamp_ms
        , c.block_date
        , c.block_month
        , c.checkpoint
        , c.owner_type
        , c.receiver
        , c.coin_type
        , c.object_status
        , c.coin_balance
        , c.prev_owner
        , c.prev_balance
        , s.tx_sender
        , c.coin_balance - coalesce(c.prev_balance, 0) as balance_delta
        , case
            when c.object_status in ('Created', 'Deleted') then false
            when c.object_status = 'Mutated'
                and c.prev_owner is not null
                and c.prev_owner != c.receiver then true
            else false
        end as has_ownership_change
    from calc c
    left join tx_senders s
        on c.tx_digest = s.tx_digest
    where c.object_status != 'ANCHOR'
)
, filtered as (
    select
        e.*
    from enriched e
    where e.balance_delta != 0
        or e.has_ownership_change
)
, coin_metadata as (
    select
        m.coin_type
        , m.coin_symbol
        , m.coin_decimals
    from {{ ref('dex_sui_coin_info') }} m
)
, cctp_supply_txs as (
    select distinct
        e.transaction_digest as tx_digest
        , case
            when e.event_type like '%message_transmitter%MessageReceived%' then 'mint'
            when e.event_type like '%DepositForBurn%' then 'burn'
        end as supply_event_type
    from {{ source('sui', 'events') }} e
    inner join (
        select distinct f.tx_digest
        from filtered f
    ) ftx
        on e.transaction_digest = ftx.tx_digest
    where (
            e.event_type like '%message_transmitter%MessageReceived%'
            or e.event_type like '%DepositForBurn%'
        )
        and e.date >= date '{{ sui_transfer_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('e.date') }}
        {% endif %}
)
, tx_reconciliation as (
    select
        f.tx_digest
        , f.coin_type
        , sum(f.balance_delta) as tx_net_delta
        , count(distinct f.receiver) as tx_distinct_receivers
        , count(distinct f.prev_owner) filter (where f.prev_owner is not null) as tx_distinct_senders
        , bool_or(f.balance_delta > 0) and bool_or(f.balance_delta < 0) as tx_has_bidirectional_deltas
    from filtered f
    group by f.tx_digest, f.coin_type
)

select
    {{ dbt_utils.generate_surrogate_key(['f.tx_digest', 'f.object_id', 'f.version']) }} as unique_key
    , 'sui' as blockchain
    , f.block_month
    , f.block_date
    , from_unixtime(f.timestamp_ms / 1000) as block_time
    , f.checkpoint as block_number
    , f.tx_digest as tx_hash
    , cast(null as bigint) as evt_index
    , cast(null as array(bigint)) as trace_address
    , 'sui_coin' as token_standard
    , f.tx_sender as tx_from
    , cast(null as varbinary) as tx_to
    , cast(null as bigint) as tx_index
    , case
        when f.object_status = 'Created' then f.tx_sender
        else coalesce(f.prev_owner, f.tx_sender)
    end as "from"
    , f.receiver as to
    , cast(split_part(f.coin_type, '::', 1) as varchar) as contract_address
    , f.coin_type as contract_address_full
    , m.coin_symbol as symbol
    , m.coin_decimals as decimals
    , case
        when f.object_status = 'Created' then f.coin_balance
        when f.has_ownership_change and f.balance_delta = 0 then f.coin_balance
        else abs(f.balance_delta)
    end as amount_raw
    , f.balance_delta
    , f.object_id
    , f.version
    , f.object_status
    , f.owner_type
    , f.coin_balance
    , f.prev_balance
    , f.prev_owner
    , f.has_ownership_change
    , case
        when f.object_status = 'Created' then 'object_created'
        when f.object_status = 'Deleted' then 'object_deleted'
        when f.has_ownership_change and f.balance_delta != 0 then 'transfer_with_balance_change'
        when f.has_ownership_change then 'ownership_transfer'
        when f.balance_delta > 0 then 'balance_increase'
        when f.balance_delta < 0 then 'balance_decrease'
        else 'other'
    end as transfer_type
    , case
        when f.object_status = 'Created' then f.tx_sender
        else coalesce(f.prev_owner, f.tx_sender)
    end != f.receiver as is_cross_address_transfer
    , cctp.supply_event_type is not null as is_supply_event
    , cctp.supply_event_type as supply_event_type
    , case
        when f.balance_delta > 0 then 'credit'
        when f.balance_delta < 0 then 'debit'
        else 'neutral'
    end as transfer_direction
    , r.tx_net_delta
    , r.tx_distinct_receivers
    , r.tx_distinct_senders
    , r.tx_has_bidirectional_deltas
from filtered f
left join tx_reconciliation r
    on f.tx_digest = r.tx_digest
    and f.coin_type = r.coin_type
left join cctp_supply_txs cctp
    on f.tx_digest = cctp.tx_digest
left join coin_metadata m
    on lower(f.coin_type) = m.coin_type
