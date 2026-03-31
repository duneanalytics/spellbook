{{ config(
        schema = 'world_megaeth',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{% set project_start_date = '2026-02-01' %}
{% set composite_exchange = '0x5e3ae52eba0f9740364bd5dd39738e1336086a8b' %}

with decoded_calls as (
    select
        td.block_time
        , td.tx_hash
        , td.trace_address
        , td.to as project_contract_address
        , row_number() over (
            partition by td.tx_hash
            order by coalesce(cardinality(td.trace_address), 0), cast(td.trace_address as varchar)
        ) as tx_trace_rank
    from {{ source('megaeth', 'traces_decoded') }} as td
    where lower(td.namespace) = 'world'
        and lower(td.contract_name) = 'compositeexchange'
        and td.to = {{ composite_exchange }}
        {% if is_incremental() %}
        and {{ incremental_predicate('td.block_time') }}
        {% else %}
        and td.block_time >= timestamp '{{ project_start_date }}'
        {% endif %}
)
, composite_calls as (
    select
        block_time
        , tx_hash
        , trace_address
        , project_contract_address
    from decoded_calls
    where tx_trace_rank = 1
)
, txs as (
    select
        tx.hash as tx_hash
        , tx.block_time
        , tx."from" as tx_from
        , tx.to as tx_to
    from {{ source('megaeth', 'transactions') }} as tx
    inner join composite_calls as c
        on c.tx_hash = tx.hash
    {% if is_incremental() %}
    where {{ incremental_predicate('tx.block_time') }}
    {% else %}
    where tx.block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
)
, transfer_legs as (
    select
        t.evt_tx_hash as tx_hash
        , t."from" as from_address
        , t."to" as to_address
        , t.contract_address as token_address
        , cast(t.value as uint256) as amount_raw
    from {{ source('erc20_megaeth', 'evt_Transfer') }} as t
    inner join composite_calls as c
        on c.tx_hash = t.evt_tx_hash
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% else %}
    where t.evt_block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
        and t.value > 0
)
, sold_candidates as (
    select
        l.tx_hash
        , l.token_address as token_sold_address
        , l.amount_raw as token_sold_amount_raw
        , row_number() over (
            partition by l.tx_hash
            order by l.amount_raw desc, l.token_address
        ) as amount_rank
    from transfer_legs as l
    inner join txs as tx
        on tx.tx_hash = l.tx_hash
    where l.from_address = tx.tx_from
        and l.to_address <> tx.tx_from
)
, bought_candidates as (
    select
        l.tx_hash
        , l.token_address as token_bought_address
        , l.amount_raw as token_bought_amount_raw
        , row_number() over (
            partition by l.tx_hash
            order by l.amount_raw desc, l.token_address
        ) as amount_rank
    from transfer_legs as l
    inner join txs as tx
        on tx.tx_hash = l.tx_hash
    where l.to_address = tx.tx_from
        and l.from_address <> tx.tx_from
)

select
    'megaeth' as blockchain
    , 'world' as project
    , '1' as version
    , cast(date_trunc('day', c.block_time) as date) as block_date
    , cast(date_trunc('month', c.block_time) as date) as block_month
    , c.block_time
    , b.token_bought_amount_raw
    , s.token_sold_amount_raw
    , b.token_bought_address
    , s.token_sold_address
    , tx.tx_from as taker
    , cast(null as varbinary) as maker
    , c.project_contract_address
    , c.tx_hash
    , tx.tx_from
    , tx.tx_to
    , coalesce(c.trace_address, cast(array[-1] as array<bigint>)) as trace_address
    , cast(-1 as integer) as evt_index
from composite_calls as c
inner join txs as tx
    on tx.tx_hash = c.tx_hash
left join sold_candidates as s
    on s.tx_hash = c.tx_hash
    and s.amount_rank = 1
left join bought_candidates as b
    on b.tx_hash = c.tx_hash
    and b.amount_rank = 1
where s.token_sold_address is not null
    and b.token_bought_address is not null
    and s.token_sold_address <> b.token_bought_address
