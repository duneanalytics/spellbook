CREATE OR REPLACE VIEW erc20.view_token_balances_hourly AS
SELECT 
wallet_address,
token_address,
t.symbol as token_symbol,
amount_raw,
amount_raw / 10^coalesce(t.decimals, null) amount,
amount_raw / 10^coalesce(t.decimals, null) * p.price amount_usd,
generate_series(min(timestamp), date_trunc('hour', NOW()), '1 hour') as hour
FROM erc20.token_balances
left join erc20.tokens t on t.contract_address = token_address
left join prices.usd p on p.contract_address = token_address and p.minute = timestamp
group by 1, 2, 3, 4, 5, 6
