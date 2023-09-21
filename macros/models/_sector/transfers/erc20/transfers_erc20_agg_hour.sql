{% macro transfers_erc20_agg_hour(transfers_erc20, tokens_erc20) %}

SELECT
    tr.blockchain,
    CAST(date_trunc('hour', tr.evt_block_time) as date) as block_hour,
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
WHERE tr.evt_block_time >= date_trunc('hour', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6

{% endmacro %}