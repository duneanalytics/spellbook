{% macro delta_v1_master(blockchain) %}
with
    {{ delta_settle_swap(blockchain) }},
    {{ delta_safe_settle_batch_swap(blockchain) }},
    delta_v1_master as (
        (select * from delta_v1_settleSwap)
            union all   
        (select * from delta_v1_safeSettleBatch)
    )
    {{ map_internal_to_dex(blockchain, 'v1', 'delta_v1_master') }} 
{% endmacro %}