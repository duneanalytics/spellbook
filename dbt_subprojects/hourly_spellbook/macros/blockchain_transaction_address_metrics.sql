{% macro blockchain_transaction_address_metrics(blockchain) %}

with contracts as (
    select
        distinct address
    from
        {{ source(blockchain, 'creation_traces') }}
    where
        1 = 1
        and block_time >= timestamp '2024-08-01'
)
, from_new_address as (
    select
        "from" as address
        , min(date_trunc('hour', block_time)) as min_block_hour
    from
        {{ source(blockchain, 'transactions') }}
    where
        1 = 1
        and block_time >= timestamp '2024-08-01'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by
        1
)
, to_new_address as (
    select
        to as address
        , min(date_trunc('hour', block_time)) as min_block_hour
    from
        {{ source(blockchain, 'transactions') }}
    where
        1 = 1
        and block_time >= timestamp '2024-08-01'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by
        1
)
, tx as (
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
        and block_time >= timestamp '2024-08-01'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by
        1
        , 2
        , 3
        , 4
)
select
    tx.blockchain
    , ei.chain_id
    , tx.block_hour
    , tx.from_address
    , tx.to_address
    , tx.tx_count
    , cast(tx.tx_success_count as double)/cast(tx.tx_count as double) as tx_success_rate
    , case
        when tx.block_hour = from_new_address.min_block_hour then True
        else False
        end as from_is_new_address
    , case
        when from_contract.address is not null then True
        else False
        end as from_is_contract
    , case
        when tx.block_hour = to_new_address.min_block_hour then True
        else False
        end as to_is_new_address
    , case
        when to_contract.address is not null then True
        else False
        end as to_is_contract
from
    tx
inner join
    {{ source('evms', 'info') }} as ei
    on '{{ blockchain }}' = ei.blockchain
inner join
    from_new_address
    on tx.from_address = from_new_address.address
inner join
    to_new_address
    on tx.to_address = to_new_address.address
left join
    contracts as from_contract
    on tx.from_address = from_contract.address
left join
    contracts as to_contract
    on tx.to_address = to_contract.address
    
{% endmacro %}