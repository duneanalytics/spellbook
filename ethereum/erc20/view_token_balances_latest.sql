CREATE OR REPLACE VIEW erc20.view_token_balances_latest AS
SELECT distinct on (wallet_address, token_address)
wallet_address,
token_address,
t.symbol as token_symbol,
amount_raw,
amount_raw / 10^coalesce(t.decimals, 0) amount_formatted,
amount_raw / 10^coalesce(t.decimals, 0) * p.price amount_usd,
timestamp
FROM erc20.token_balances
left join erc20.tokens t on t.contract_address = token_address
left join prices.usd p on p.contract_address = token_address and p.minute = date_trunc('hour', NOW())
order by wallet_address, token_address, timestamp desc
