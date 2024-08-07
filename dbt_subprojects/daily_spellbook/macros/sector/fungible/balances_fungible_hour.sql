{% macro balances_fungible_hour(blockchain, transfers_rolling_hour, first_transaction_date, is_more_than_year_ago, balances_noncompliant=null, filter_mainnet_token=null ) %}

WITH 

{% if is_more_than_year_ago %}
    -- This block will execute if first_transaction_date is more than a year ago
    years as (
        select year
        from (values (sequence(CAST('{{first_transaction_date}}' as timestamp), cast(date_trunc('year', now()) as timestamp), interval '1' year))) s(year_array)
        cross join unnest(year_array) as d(year)
    ),
    hours as (
        select date_add('hour', s.n, y.year) as block_hour
        from years y
        cross join unnest(sequence(1, 9000)) s(n)
        where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
    ),
{% else %}
    -- This block will execute if first_transaction_date is within the last year
    time_seq AS (
        SELECT 
            sequence(
            CAST('{{first_transaction_date}}' as timestamp),
            date_trunc('hour', cast(now() as timestamp)),
            interval '1' hour
            ) AS time 
    ),
    hours AS (
        SELECT 
            time.time AS block_hour 
        FROM time_seq
        CROSS JOIN unnest(time) AS time(time)
    ),
{% endif %}

hourly_balances as (
    SELECT
        blockchain, 
        block_hour, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(block_hour, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY block_hour) AS next_hour
    FROM 
    {{ transfers_rolling_hour }}
)

SELECT
    b.blockchain,
    cast(date_trunc('month', d.block_hour) as date) as block_month,
    d.block_hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM 
hourly_balances b
INNER JOIN 
hours d 
    ON b.block_hour <= d.block_hour
    AND d.block_hour < b.next_hour
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.block_hour = p.minute
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