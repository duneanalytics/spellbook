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
        , o.owner_address as receiver
        , o.coin_type
        , o.object_status
        , try_cast(o.coin_balance as bigint) as coin_balance
    from {{ source('sui', 'objects') }} o
    where o.object_status in ('Created', 'Mutated')
        and o.owner_type = 'AddressOwner'
        and o.coin_type is not null
        and o.coin_type = '{{ sui_transfer_coin_type }}'
        and o.date >= date '{{ sui_transfer_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('o.date') }}
        {% endif %}
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
        , max_by(p.owner_address, p.version) as receiver
        , p.coin_type
        , cast('ANCHOR' as varchar) as object_status
        , max_by(try_cast(p.coin_balance as bigint), p.version) as coin_balance
    from {{ source('sui', 'objects') }} p
    where p.object_status in ('Created', 'Mutated')
        and p.owner_type = 'AddressOwner'
        and p.coin_type is not null
        and p.coin_type = '{{ sui_transfer_coin_type }}'
        and p.date < date '{{ sui_transfer_start_date }}'
        and p.object_id in (select distinct d.object_id from day_rows d)
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
        , d.receiver
        , d.coin_type
        , d.object_status
        , d.coin_balance
    from day_rows d
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
        , u.receiver
        , u.coin_type
        , u.object_status
        , u.coin_balance
        , lag(u.receiver) over (partition by u.object_id order by u.version) as prev_owner
        , lag(u.coin_balance) over (partition by u.object_id order by u.version) as prev_balance
    from unioned u
)
, tx_senders as (
    select
        e.transaction_digest as tx_digest
        , max_by(e.sender, e.event_index) as tx_sender
    from {{ source('sui', 'events') }} e
    inner join (
        select distinct
            c.tx_digest
        from calc c
        where c.tx_digest is not null
    ) d
        on e.transaction_digest = d.tx_digest
    group by e.transaction_digest
)
, calc_with_sender as (
    select
        c.object_id
        , c.version
        , c.tx_digest
        , c.timestamp_ms
        , c.block_date
        , c.block_month
        , c.checkpoint
        , c.receiver
        , c.coin_type
        , c.object_status
        , c.coin_balance
        , c.prev_owner
        , c.prev_balance
        , s.tx_sender
    from calc c
    left join tx_senders s
        on c.tx_digest = s.tx_digest
)
, outs as (
    select
        c.tx_digest
        , c.receiver
        , c.coin_type
        , sum(
            case
                when c.object_status = 'Created' then c.coin_balance
                when c.object_status = 'Mutated' then greatest(c.coin_balance - coalesce(c.prev_balance, 0), 0)
                else 0
            end
        ) as amount_raw
    from calc_with_sender c
    where (
            c.object_status = 'Created'
            and c.coin_balance > 0
        ) or (
            c.object_status = 'Mutated'
            and (c.coin_balance - coalesce(c.prev_balance, 0)) > 0
        )
    group by c.tx_digest, c.receiver, c.coin_type
)
, tx_stats as (
    select
        o.tx_digest
        , count(*) as receiver_cnt
    from outs o
    group by o.tx_digest
)
, coin_metadata as (
    select
        m.coin_type
        , m.coin_symbol
        , m.coin_decimals
    from {{ ref('dex_sui_coin_info') }} m
)

select
    {{ dbt_utils.generate_surrogate_key(['c.tx_digest', 'c.object_id', 'c.version']) }} as unique_key
    , 'sui' as blockchain
    , c.block_month
    , c.block_date
    , from_unixtime(c.timestamp_ms / 1000) as block_time
    , c.checkpoint as block_number
    , c.tx_digest as tx_hash
    , cast(null as bigint) as evt_index
    , cast(null as array(bigint)) as trace_address
    , 'sui_coin' as token_standard
    , c.tx_sender as tx_from
    , cast(null as varbinary) as tx_to
    , cast(null as bigint) as tx_index
    , case
        when c.object_status = 'Created' then c.tx_sender
        else c.prev_owner
    end as "from"
    , c.receiver as to
    , cast(split_part(c.coin_type, '::', 1) as varchar) as contract_address
    , c.coin_type as contract_address_full
    , m.coin_symbol as symbol
    , m.coin_decimals as decimals
    , case
        when c.object_status = 'Created' then c.coin_balance
        else greatest(c.coin_balance - coalesce(c.prev_balance, 0), 0)
    end as amount_raw
    , c.object_id
    , c.version
    , c.object_status
    , c.coin_balance
    , c.prev_balance
    , case
        when ts.receiver_cnt = 1 then 'transfer'
        else 'other'
    end as classification
    , case
        when ts.receiver_cnt = 1 then true
        else false
    end as is_transfer
from calc_with_sender c
left join tx_stats ts
    on c.tx_digest = ts.tx_digest
left join coin_metadata m
    on lower(c.coin_type) = m.coin_type
where (
        c.object_status = 'Created'
        and c.coin_balance > 0
    ) or (
        c.object_status = 'Mutated'
        and (c.coin_balance - coalesce(c.prev_balance, 0)) > 0
    )
