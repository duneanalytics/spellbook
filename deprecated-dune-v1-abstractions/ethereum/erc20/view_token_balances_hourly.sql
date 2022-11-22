CREATE OR REPLACE VIEW erc20.view_token_balances_hourly AS
with hours as (
    select hour from generate_series('2015-01-01', date_trunc('hour', NOW()), '1 hour') g(hour)
),
b as (
    SELECT 
    tb.wallet_address,
    tb.token_address,
    tb.amount_raw,
    timestamp as hour,
    lead(date_trunc('hour', timestamp), 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY date_trunc('hour', timestamp)) AS next_hour 
    FROM erc20.token_balances tb
)
SELECT 
    h.hour, 
    b.wallet_address, 
    b.token_address, 
    b.amount_raw,
    t.symbol as token_symbol,
    amount_raw / 10^t.decimals amount,
    amount_raw / 10^t.decimals * p.price amount_usd
FROM b
INNER JOIN hours h ON b.hour <= h.hour AND h.hour < b.next_hour
left join erc20.tokens t on t.contract_address = token_address
left join prices.usd p on p.contract_address = token_address and p.minute = h.hour
