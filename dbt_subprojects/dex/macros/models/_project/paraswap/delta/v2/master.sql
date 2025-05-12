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
    select 
        delta_v2_master.blockchain,
        'velora_delta' as project,
        'v2' as version,
        block_month,
        DATE_TRUNC('day', call_block_time) as block_date,
        call_block_time as block_time,
        t_dest_token.symbol as token_bought_symbol,
        t_src_token.symbol as token_sold_symbol,
        CASE
            WHEN lower(t_dest_token.symbol) > lower(t_src_token.symbol)
            THEN concat(t_src_token.symbol, '-', t_dest_token.symbol)
            ELSE concat(t_dest_token.symbol, '-', t_src_token.symbol)
        END as token_pair,
        dest_amount / power(10, t_dest_token.decimals) as token_bought_amount,
        src_amount / power(10, t_src_token.decimals) as token_sold_amount,
        dest_amount as token_bought_amount_raw,
        src_amount as token_sold_amount_raw,
        COALESCE(dest_token_order_usd, src_token_order_usd) as amount_usd,
        dest_token as token_bought_address,
        src_token as token_sold_address,
        owner as taker,        
        CAST(NULL AS VARBINARY) AS maker, -- TODO: consider `executor as maker`,
        delta_v2_master.contract_address as project_contract_address,
        call_tx_hash as tx_hash,
        call_tx_from as tx_from,
        call_tx_to as tx_to,
        call_trace_address as trace_address,
        evt_index,
        order_index,
        method
    from delta_v2_master  
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_src_token 
            ON t_src_token.blockchain = '{{blockchain}}'
            AND t_src_token.contract_address = src_token
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_dest_token
            ON t_dest_token.blockchain = '{{blockchain}}'
            AND t_dest_token.contract_address = dest_token
{% endmacro %}