CREATE OR REPLACE VIEW erc20.view_token_balances_latest AS
SELECT distinct on (wallet_address, token_address)
wallet_address,
token_address,
t.symbol as token_symbol,
amount_raw,
amount_raw / 10^coalesce(t.decimals, null) amount,
amount_raw / 10^coalesce(t.decimals, null) * p.price amount_usd,
timestamp as last_transfer_timestamp
FROM erc20.token_balances
left join erc20.tokens t on t.contract_address = token_address
left join prices.usd p on p.contract_address = token_address and p.minute = date_trunc('hour', NOW())
order by wallet_address, token_address, timestamp desc
