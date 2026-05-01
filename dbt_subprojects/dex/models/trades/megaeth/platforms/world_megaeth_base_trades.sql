{{
    config(
        schema = 'world_megaeth',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2026-02-01' %}
{% set composite_exchange = '0x5e3ae52eba0f9740364bd5dd39738e1336086a8b' %}

with base_events as (
    select
        e.evt_block_date as block_date
        , e.evt_block_time as block_time
        , e.evt_block_number as block_number
        , e.evt_tx_hash as tx_hash
        , e.evt_index
        , e.contract_address as project_contract_address
    from {{ source('world_megaeth', 'compositeexchange_evt_orderbookregistered') }} e
    where e.contract_address = {{ composite_exchange }}
    {% if is_incremental() %}
        and {{ incremental_predicate('e.evt_block_time') }}
    {% else %}
        and e.evt_block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
)

, txs as (
    select
        tx.hash as tx_hash
        , tx.block_date
        , tx.block_number
        , tx."from" as tx_from
    from {{ source('megaeth', 'transactions') }} tx
    inner join base_events be
        on be.tx_hash = tx.hash
        and be.block_date = tx.block_date
        and be.block_number = tx.block_number
    {% if is_incremental() %}
    where {{ incremental_predicate('tx.block_time') }}
    {% else %}
    where tx.block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
)

, transfer_legs as (
    select
        t.evt_tx_hash as tx_hash
        , t.evt_block_date as block_date
        , t.evt_block_number as block_number
        , t."from" as from_address
        , t."to" as to_address
        , t.contract_address as token_address
        , cast(t.value as uint256) as amount_raw
    from {{ source('erc20_megaeth', 'evt_Transfer') }} t
    inner join base_events be
        on be.tx_hash = t.evt_tx_hash
        and be.block_date = t.evt_block_date
        and be.block_number = t.evt_block_number
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
        , l.block_date
        , l.block_number
        , l.token_address as token_sold_address
        , l.amount_raw as token_sold_amount_raw
        , row_number() over (
            partition by l.tx_hash, l.block_date, l.block_number
            order by l.amount_raw desc, l.token_address
        ) as amount_rank
    from transfer_legs l
    inner join txs tx
        on tx.tx_hash = l.tx_hash
        and tx.block_date = l.block_date
        and tx.block_number = l.block_number
    where l.from_address = tx.tx_from
        and l.to_address <> tx.tx_from
)

, bought_candidates as (
    select
        l.tx_hash
        , l.block_date
        , l.block_number
        , l.token_address as token_bought_address
        , l.amount_raw as token_bought_amount_raw
        , row_number() over (
            partition by l.tx_hash, l.block_date, l.block_number
            order by l.amount_raw desc, l.token_address
        ) as amount_rank
    from transfer_legs l
    inner join txs tx
        on tx.tx_hash = l.tx_hash
        and tx.block_date = l.block_date
        and tx.block_number = l.block_number
    where l.to_address = tx.tx_from
        and l.from_address <> tx.tx_from
)

select
    'megaeth' as blockchain
    , 'world' as project
    , '1' as version
    , cast(date_trunc('month', be.block_time) as date) as block_month
    , cast(date_trunc('day', be.block_time) as date) as block_date
    , be.block_time
    , be.block_number
    , b.token_bought_amount_raw
    , s.token_sold_amount_raw
    , b.token_bought_address
    , s.token_sold_address
    , tx.tx_from as taker
    , cast(null as varbinary) as maker
    , be.project_contract_address
    , be.tx_hash
    , be.evt_index
from base_events be
inner join txs tx
    on tx.tx_hash = be.tx_hash
    and tx.block_date = be.block_date
    and tx.block_number = be.block_number
left join sold_candidates s
    on s.tx_hash = be.tx_hash
    and s.block_date = be.block_date
    and s.block_number = be.block_number
    and s.amount_rank = 1
left join bought_candidates b
    on b.tx_hash = be.tx_hash
    and b.block_date = be.block_date
    and b.block_number = be.block_number
    and b.amount_rank = 1
where s.token_sold_address is not null
    and b.token_bought_address is not null
    and s.token_sold_address <> b.token_bought_address
