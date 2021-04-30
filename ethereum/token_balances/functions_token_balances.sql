create or replace function token_supply_over_time(from_ timestamptz, until_ timestamptz,token_ varchar)
   	returns table (
		time_ timestamptz,
		balance numeric
	)
   language plpgsql
  as
$$
declare
-- variable declaration
begin
RETURN QUERY
with hours AS (
    SELECT generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
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
	where token = token_
)
, balance_all_days AS (
    SELECT  d.hour_ as time_,
            sum(amount) AS balance
    FROM token_balances_updated  b
    INNER JOIN hours d ON b.ts <= d.hour_ AND d.hour_ < b.next_hour
    group by 1
    )

    select * from balance_all_days
   ;
end;
$$



-- token wallets in a point in time


create or replace function token_addresses_over_time(from_ timestamptz, until_ timestamptz,token_ varchar)
   	returns table (
		time_ timestamptz,
		selected_token varchar ,
        wallet_address bytea
	)
   language plpgsql
  as
$$
declare
-- variable declaration
begin
RETURN QUERY
with hours AS (
    SELECT generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
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
	where token = token_
)
, balance_all_days AS (
    SELECT  d.hour_ as time_,
    		token,
             address
    FROM token_balances_updated  b
    INNER JOIN hours d ON b.ts <= d.hour_ AND d.hour_ < b.next_hour
    --group by 1,2
    )

    select * from balance_all_days
   ;
end;
$$





    create or replace function token_balance_per_address(from_ timestamptz,until_ timestamptz,  address_ bytea)
   	returns table (
		time_ timestamptz,
		address__ bytea,
		balance numeric
	)
   language plpgsql
  as
$$
declare
-- variable declaration
begin
RETURN QUERY
with hours AS (
    SELECT generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
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
	where address = address_
)
, balance_all_days AS (
    SELECT  d.hour_ as time_,
    address,
            sum(amount) AS balance
    FROM token_balances_updated  b
    INNER JOIN hours d ON b.ts <= d.hour_ AND d.hour_ < b.next_hour
    group by 1,2
    )

    select * from balance_all_days
   ;
end;
$$
   ;