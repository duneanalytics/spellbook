{% macro transfers_fungible_agg_day(transfers_erc20=null, tokens_erc20=null, transfers_native=null, native_token_symbol = null ) %}

-- erc20 balances
{% if transfers_erc20 and tokens_erc20 %}
SELECT
    tr.blockchain,
    CAST(date_trunc('day', tr.evt_block_time) as date) as block_day,
    tr.block_month,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
FROM 
{{ transfers_erc20 }} tr
LEFT JOIN 
{{ tokens_erc20 }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.evt_block_time >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6
{% endif %}

-- native token balances
{% if transfers_native and native_token_symbol %}
SELECT
    tr.blockchain,
    CAST(date_trunc('day', tr.block_time) as date) as block_day,
    block_month,
    tr.wallet_address,
    tr.token_address,
    '{{native_token_symbol}}' as symbol,
    SUM(tr.amount_raw) as amount_raw,
    SUM(tr.amount_raw / power(10, 18)) as amount
FROM 
{{ transfers_native }} tr
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.block_time >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6
{% endif %}

{% endmacro %}