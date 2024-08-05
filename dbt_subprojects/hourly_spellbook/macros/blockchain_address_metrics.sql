{% macro blockchain_address_metrics(blockchain) %}

with run as (
select
    '{{blockchain}}' as blockchain
    ,"from" as address
    ,min(block_time) as min_block_time
from {{ source(blockchain,'transactions') }} tx
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
group by 1,2
)


{% if is_incremental() %}
select
    coalesce(o.blockchain, r.blockchain) as blockchain
    ,coalesce(o.address, r.address) as address
    ,coalesce(o.min_block_time, r.min_block_time) as min_block_time
from run r
left join {{this}} o
on r.blockchain = o.blockchain
and r.address = o.address
{% else %}
select * from run
{% endif %}


{% endmacro %}

