{% macro balances_fungible_latest(blockchain, transfers_rolling_hour, balances_noncompliant=null, filter_mainnet_token=null ) %}

SELECT
    rh.wallet_address,
    rh.token_address,
    rh.amount_raw,
    rh.amount,
    rh.amount * p.price as amount_usd,
    rh.symbol,
    rh.last_updated
FROM 
{{ transfers_rolling_hour }} rh
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = rh.token_address
    AND p.minute = date_trunc('minute', rh.last_updated) - Interval '10' Minute
    AND p.blockchain = '{{blockchain}}'
-- Removes likely non-compliant tokens due to negative balances
{% if balances_noncompliant %}
LEFT JOIN {{ balances_noncompliant }} nc
    ON rh.token_address = nc.token_address
{% endif %}
WHERE rh.recency_index = 1
{% if balances_noncompliant %}
AND nc.token_address IS NULL
{% endif %}
-- Removes mainnet token for chains which have erc20 transfer event for mainnet tokens (eg optimism)
{% if filter_mainnet_token %}
AND rh.token_address != {{filter_mainnet_token}}
{% endif %}

{% endmacro %}
