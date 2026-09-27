-- Check the chosen transfer within each first-funding transaction. This runs
-- after both the initial and incremental CI builds, including equal-tx ties.
with expected as (
    select
        f.address
        , min_by(t."from", coalesce(t.evt_index, -1)) as first_funded_by
        , min_by(t.amount, coalesce(t.evt_index, -1)) as amount
        , min_by(t.amount_usd, coalesce(t.evt_index, -1)) as amount_usd
        , min_by(t.unique_key, coalesce(t.evt_index, -1)) as unique_key
    from {{ source('tokens_arc', 'transfers') }} t
    inner join {{ ref('addresses_events_arc_first_funded_by') }} f
        on t.to = f.address
        and t.tx_hash = f.tx_hash
        and t.block_number = f.block_number
        and t.block_date = cast(f.block_time as date)
    where t.token_standard = 'native'
        and {{ incremental_predicate('t.block_time') }}
    group by f.address
)

select f.address
from {{ ref('addresses_events_arc_first_funded_by') }} f
inner join expected e on f.address = e.address
where f.first_funded_by is distinct from e.first_funded_by
    or f.amount is distinct from e.amount
    or f.amount_usd is distinct from e.amount_usd
    or f.unique_key is distinct from e.unique_key
    or f.trace_address is not null
