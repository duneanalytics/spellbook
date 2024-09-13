{% macro blockchain_transaction_metrics(blockchain) %}

WITH blocks as (
    select
    '{{ blockchain }}' as blockchain
    ,date_trunc('hour',"time") as block_hour
    ,avg(date_diff('second',lag_time,"time")) as avg_block_time_seconds
from (
    select
    time
    ,lag(time) over (order by "time" asc) as lag_time
    from {{source(blockchain,'blocks')}}
    {% if is_incremental() %}
    where {{ incremental_predicate('time') }}
    {% endif %}
)
group by 1,2
)

, transactions as (
    select
    '{{ blockchain }}' as blockchain
    ,date_trunc('hour',block_time) as block_hour
    ,count(hash) as tx_count
    ,count_if(success) as tx_success_count
    ,cast(count(hash) as double)/(60.0*60.0) as tx_per_second
from {{source(blockchain,'transactions')}}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
group by 1,2
)

, new_addresses as (
    select
    '{{blockchain}}' as blockchain
    ,date_trunc('hour',min_block_time) as block_hour
    ,count(distinct address) as new_addresses
from {{ref(blockchain ~ '_address_metrics')}}
{% if is_incremental() %}
where {{ incremental_predicate('min_block_time') }}
{% endif %}
group by 1,2
)

, new_contracts as (
    select
    '{{ blockchain }}' as blockchain
    ,date_trunc('hour',block_time) as block_hour
    ,count(distinct address) as new_contracts
from {{source(blockchain,'creation_traces')}}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
group by 1,2
)


select
    blockchain
    ,chain_id
    ,block_hour
    ,tx_count
    ,tx_success_count
    ,cast(tx_success_count as double)/cast(tx_count as double) as tx_success_rate
    ,avg_block_time_seconds
    ,tx_per_second
    ,new_addresses
    ,new_contracts
from blocks
left join transactions using (blockchain, block_hour)
left join new_addresses using (blockchain, block_hour)
left join new_contracts using (blockchain, block_hour)
left join {{ source('evms','info') }} using (blockchain)
{% endmacro %}
