{% macro transfers_erc20_agg_day(transfers_erc20, tokens_erc20, unique_transfer_id=false) %}

SELECT
    tr.blockchain,
    CAST(date_trunc('day', tr.evt_block_time) as date) as block_day,
    tr.block_month,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
    {% if unique_transfer_id %}
    , cast(tr.wallet_address as varchar) || '-' || cast(tr.token_address as varchar) || '-' || cast(date_trunc('day', tr.evt_block_time) as varchar) as unique_transfer_id
    {% endif %}
FROM 
{{ transfers_erc20 }} tr
LEFT JOIN 
{{ tokens_erc20 }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE tr.evt_block_time >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6{% if unique_transfer_id %}, 9{% endif %}

{% endmacro %}
