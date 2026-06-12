{% macro blockchain_transaction_address_metrics(blockchain) %}

with tx as (
    select
        '{{ blockchain }}' as blockchain
        , date_trunc('hour', block_time) as block_hour
        , "from" as from_address
        , to as to_address
        , count(hash) as tx_count
        , count_if(success) as tx_success_count
    from
        {{ source(blockchain, 'transactions') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by
        1
        , 2
        , 3
        , 4
)
{#
  New-address flags come from window mins over the single tx scan instead of two
  extra transactions scans + joins. The mins are computed before the null filter
  so they see the same row set as the old from_new_address/to_new_address CTEs
  (contract creations have a null `to` but still count toward `from` activity).
  The null filter itself mirrors the old inner joins, which dropped null keys.
#}
, tx_flagged as (
    select *
    from (
        select
            tx.*
            , tx.block_hour = min(tx.block_hour) over (partition by tx.from_address) as from_is_new_address
            , tx.block_hour = min(tx.block_hour) over (partition by tx.to_address) as to_is_new_address
        from tx
    )
    where
        from_address is not null
        and to_address is not null
)
{#
  Fan each row out to its two addresses so creation_traces is scanned once
  instead of twice. No distinct on creation_traces: duplicate created addresses
  only duplicate fanned rows, and the final group by absorbs them.
#}
, fanned as (
    select
        t.blockchain
        , t.block_hour
        , t.from_address
        , t.to_address
        , t.tx_count
        , t.tx_success_count
        , t.from_is_new_address
        , t.to_is_new_address
        , side.is_from
        , (ct.address is not null) as side_is_contract
    from
        tx_flagged as t
    cross join unnest(array[t.from_address, t.to_address], array[true, false]) as side(side_address, is_from)
    left join {{ source(blockchain, 'creation_traces') }} as ct
        on side.side_address = ct.address
)
select
    f.blockchain
    , ei.chain_id
    , f.block_hour
    , f.from_address
    , f.to_address
    , f.tx_count
    , cast(f.tx_success_count as double)/cast(f.tx_count as double) as tx_success_rate
    , f.from_is_new_address
    , bool_or(f.is_from and f.side_is_contract) as from_is_contract
    , f.to_is_new_address
    , bool_or((not f.is_from) and f.side_is_contract) as to_is_contract
from
    fanned as f
inner join
    {{ source('evms', 'info') }} as ei
    on '{{ blockchain }}' = ei.blockchain
group by
    1
    , 2
    , 3
    , 4
    , 5
    , 6
    , 7
    , 8
    , 10

{% endmacro %}
