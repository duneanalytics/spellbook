{% macro balances_fungible_day(blockchain, transfers_rolling_day, first_transaction_date, balances_noncompliant=null, filter_mainnet_token=null ) %}

WITH 

time_seq AS (
    SELECT 
        sequence(
        CAST('{{first_transaction_date}}' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) AS time 
),

days AS (
    SELECT 
        time.time AS block_day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

daily_balances as (
    SELECT
        blockchain, 
        block_day, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(block_day, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY block_day) AS next_day
    FROM 
    {{ transfers_rolling_day }}
)

SELECT
    b.blockchain,
    cast(date_trunc('month', d.block_day) as date) as block_month,
    d.block_day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM 
daily_balances b
INNER JOIN 
days d 
    ON b.block_day <= d.block_day 
    AND d.block_day < b.next_day
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.block_day = p.minute
    AND p.blockchain = '{{blockchain}}'
-- Removes likely non-compliant tokens due to negative balances
{% if balances_noncompliant %}
LEFT JOIN {{ balances_noncompliant }} nc
    ON b.token_address = nc.token_address
{% endif %}
WHERE 1 = 1 
{% if balances_noncompliant %}
AND nc.token_address IS NULL
{% endif %}
-- Removes mainnet token for chains which have erc20 transfer event for mainnet tokens (eg optimism)
{% if filter_mainnet_token %}
AND b.token_address != {{filter_mainnet_token}}
{% endif %}

{% endmacro %}

