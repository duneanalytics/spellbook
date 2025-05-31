{% macro map_internal_to_dex(blockchain, version, from_alias) %}
select 
        {{from_alias}}.blockchain,
        'velora_delta' as project,
        '{{version}}' as version,
        date_trunc('month', call_block_time) AS block_month,
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
        {% if version == 'v2' %}
            {{from_alias}}.executor as maker,
        {% else %}
            CAST(NULL AS VARBINARY) as maker,
        {% endif %}
        {{from_alias}}.contract_address as project_contract_address,
        call_tx_hash as tx_hash,
        call_tx_from as tx_from,
        call_tx_to as tx_to,
        case when CARDINALITY(call_trace_address) > 0 then call_trace_address else ARRAY[-1] end as trace_address,
        COALESCE(evt_index, 0) as evt_index, -- TMP: after joining envents in swapSettle can remove it
        order_index,
        method
    from {{from_alias}}  
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_src_token 
            ON t_src_token.blockchain = '{{blockchain}}'
            AND t_src_token.contract_address = src_token
        LEFT JOIN 
        {{ source('tokens', 'erc20') }} t_dest_token
            ON t_dest_token.blockchain = '{{blockchain}}'
            AND t_dest_token.contract_address = dest_token
{% endmacro %}
