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
        delta_v2_master.blockchain,
        'velora_delta' as project,
        'v2' as version,
        block_month,
        DATE_TRUNC('day', call_block_time) as block_date,
        call_block_time as block_time,        
        CASE
            WHEN lower(t_dest_token.symbol) > lower(t_src_token.symbol)
            THEN concat(t_src_token.symbol, '-', t_dest_token.symbol)
            ELSE concat(t_dest_token.symbol, '-', t_src_token.symbol)
        END as token_pair,
        dest_amount / power(10, t_dest_token.decimals) as token_bought_amount,
        src_amount / power(10, t_src_token.decimals) as token_sold_amount        
        -- token_bought_amount_raw,
        -- token_sold_amount_raw,
        -- amount_usd,
        -- token_bought_address,
        -- token_sold_address,
        -- taker,
        -- maker,
        -- project_contract_address,
        -- tx_hash,
        -- tx_from,
        -- tx_to,
        -- trace_address,
        -- evt_index
--     FROM {{ dex_model }}
    from delta_v2_master  
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_src_token 
            ON t_src_token.blockchain = '{{blockchain}}'
            AND t_src_token.contract_address = src_token
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_dest_token
            ON t_dest_token.blockchain = '{{blockchain}}'
            AND t_dest_token.contract_address = dest_token
    
    order by block_time desc
    limit 1 
{% endmacro %}