{% macro delta_v2_master(blockchain) %}
with
    {{ delta_v2_swap_settle(blockchain) }},
    {{ delta_v2_swap_settle_batch(blockchain) }}
select
    date_trunc('month', call_block_time) AS block_month,        
    *
from delta_v2_swapSettle
    union all   
select 
    date_trunc('month', call_block_time) AS block_month,        
    *
from delta_v2_swapSettleBatch
{% endmacro %}