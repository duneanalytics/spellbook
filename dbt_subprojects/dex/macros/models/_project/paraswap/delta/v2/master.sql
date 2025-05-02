{% macro delta_v2_master(blockchain) %}
with
    {{ delta_v2_swap_settle(blockchain) }},
    {{ delta_v2_swap_settle_batch(blockchain) }},
    delta_v2_master as (
        select
            date_trunc('month', call_block_time) AS block_month,        
            *
        from delta_v2_swapSettle
            union all   
        select 
            date_trunc('month', call_block_time) AS block_month,        
            *
        from delta_v2_swapSettleBatch
    )
    select 

-- SELECT
        blockchain,
        'velora_delta' as project,
        'v2' as version,
        block_month,
        block_date,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_pair,
        token_bought_amount,
        token_sold_amount,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index
--     FROM {{ dex_model }}
    from delta_v2_master
{% endmacro %}