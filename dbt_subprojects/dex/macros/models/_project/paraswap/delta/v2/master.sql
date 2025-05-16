{% macro delta_v2_master(blockchain) %}
with
    {{ delta_v2_swap_settle(blockchain) }},
    {{ delta_v2_swap_settle_batch(blockchain) }},
    delta_v2_master as (
        (
            select
                date_trunc('month', call_block_time) AS block_month,        
                *
            from delta_v2_swapSettle
        )
            union all   
        (
            select 
                date_trunc('month', call_block_time) AS block_month,        
                *
            from delta_v2_swapSettleBatch
        )
    )
    {{ map_internal_to_dex(blockchain, 'v2', 'delta_v2_master') }} 
{% endmacro %}