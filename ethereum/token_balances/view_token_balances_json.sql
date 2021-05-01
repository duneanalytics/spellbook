create or replace view vasa.supply_per_token_per_hour_version_json as
with hours AS (
    SELECT generate_series('2015-07-01'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour_ -- Generate all days since the first contract
    )
, token_balances_updated as (
	select
	ts,
	address,
	contract_address,
	symbol as token,
	amount,
	lead(ts, 1, now()) OVER (PARTITION BY contract_address, address ORDER BY ts) AS next_hour
	from  vasa.token_balances_proposals_3 s, jsonb_to_recordset(s.token_balances) as items(amount numeric, symbol varchar,  "rawAmount" numeric, contract_address bytea)
)
, balance_all_days AS (
    SELECT  d.hour_,
            token,
            sum(amount) AS balance
    FROM token_balances_updated  b
    INNER JOIN hours d ON b.ts <= d.hour_ AND d.hour_ < b.next_hour -- Yields an observation for every day after the first transfer until the next day with transfer
    group by 1,2
    )

    select * from balance_all_days
where balance >   0.0001
   ;



--- all days


create or replace view vasa.balances_per_hour_version_json as
with hours AS (
    SELECT generate_series('2015-07-01'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour_ -- Generate all days since the first contract
    )
, token_balances_updated as (
	select
	ts,
	address,
	contract_address,
	symbol as token,
	amount,
	lead(ts, 1, now()) OVER (PARTITION BY contract_address, address ORDER BY ts) AS next_hour
	from  vasa.token_balances_proposals_3 s, jsonb_to_recordset(s.token_balances) as items(amount numeric, symbol varchar,  "rawAmount" numeric, contract_address bytea)
)
, balance_all_days AS (
    SELECT  d.hour_,
            address,
            b.token::text as symbol,
            b.contract_address,
            amount AS balance
    FROM token_balances_updated  b
    INNER JOIN hours d ON b.ts <= d.hour_ AND d.hour_ < b.next_hour -- Yields an observation for every day after the first transfer until the next day with transfer
    )

    select * from balance_all_days where balance >   0.0001;