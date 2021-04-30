
-- view to query balances over time per hour

create or replace view vasa.balances_per_hour as
with hours AS (
    SELECT generate_series('2015-07-01'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour_ -- Generate all days since the first contract
    )
, token_balances_updated as (
	select
	ts,
	address,
	contract_address,
	token,
	amount,
	lead(ts, 1, now()) OVER (PARTITION BY contract_address, address ORDER BY ts) AS next_hour
	from  vasa.token_balances_proposal_2
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

    select * from balance_all_days;


-- balance per token per hour


create or replace view vasa.balances_per__token_per_hour as
with hours AS (
    SELECT generate_series('2015-07-01'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour_ -- Generate all days since the first contract
    )
, token_balances_updated as (
	select
	ts,
	address,
	contract_address,
	token,
	amount,

	lead(ts, 1, now()) OVER (PARTITION BY contract_address, address ORDER BY ts) AS next_hour
	from  vasa.token_balances_proposal_2
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
   ;