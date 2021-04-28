

CREATE TABLE sandbox.token_balances_proposal_1(
   ts timestamptz,
   address bytea,
   contract_address bytea,
   token varchar,
   rawAmount numeric,
   amount numeric,
   usd_amount numeric,
   PRIMARY KEY( ts,address,contract_address )

);

CREATE INDEX ON sandbox.token_balances_proposal_1 USING btree (address, contract_address);
CREATE INDEX ON sandbox.token_balances_proposal_1 USING btree ( contract_address);


DO $$
DECLARE
   hour_   text;
   arr varchar[];
BEGIN
  select array(select generate_series('2021-03-21', '2021-03-23', '1 hour'::interval)::timestamptz)
    into arr;
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'another_func(%)',hour_;

	insert into sandbox.token_balances_proposal_1
        -- select evt transfer events first and unify them
		with "transfer_events" as (
		    select
		        "to" AS address,
		        tr.contract_address AS token_address,
		        value AS rawAmount
		    FROM
		        erc20. "ERC20_evt_Transfer" tr
		    WHERE  evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		    UNION ALL
		    select

		        "from" AS address,
		        tr.contract_address AS token_address,
		        - value AS rawAmount
		    FROM
		        erc20. "ERC20_evt_Transfer" tr
		    WHERE evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		)
		-- take historical balance and make sure that it is relevant to the current hour by joining on address and contract
		, "historical_balances" as (
			select
				s.address as address
				, s.contract_address as token_address
				, s.rawAmount
		    from sandbox.token_balances_proposal_1 s
		    join "transfer_events" te
		    on te.address = s.address
		    and s.contract_address = te.token_address
		    where s.ts < hour_::timestamptz
		    )
        -- unify both current hourly balance and historical balance
		, "asset_union" as (
			select * from "transfer_events"
			union all
			select * from "historical_balances"

		)
        -- calculate raw balance
		, "asset_balances" as (
		    select
		    	address,
		        token_address
		        , sum(rawAmount) AS rawAmount

		    FROM "asset_union" te
		    GROUP BY 1,2

		    )
		    ,
		    -- make sure that the balance is converted from wei, and add token symbol
		    -- join is done here to make the query more performant
		    "asset_balance_readable" as (
		    select
		    hour_::timestamptz  as ts
		    , ab.address
		    , ab.token_address as contract_address
		    , tok."symbol" as token
		    , ab.rawAmount
		    , ab.rawAmount / 10^tok."decimals" as amount
		    , 0 as usd_amount
		    from "asset_balances" ab
		    left join  erc20."tokens" tok
		    on tok."contract_address" = ab."token_address"
		   	)
            -- filter for empty balances happens here, not sure how relevant is it, need to test when dealing with accuracy
		   	select * from "asset_balance_readable"
		    where amount > 0.0001;
            -- to do -> add usd balance part, and check performance, runs 30s per day for now, but needs load
            -- prepare views as separate files

   END LOOP;
END $$;



-- function wrap
-- just informative for now, not tested, also not sure if appropriate approach

CREATE OR REPLACE FUNCTION token_balances.insert_hourly() RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE
   hour_   text;
   arr varchar[];
BEGIN
  select array(select generate_series('2021-03-21', '2021-03-23', '1 hour'::interval)::timestamptz)
    into arr;
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'another_func(%)',hour_;

	insert into sandbox.token_balances_proposal_1

		with "transfer_events" as (
		    select
		        "to" AS address,
		        tr.contract_address AS token_address,
		        value AS rawAmount
		    FROM
		        erc20. "ERC20_evt_Transfer" tr
		    WHERE  evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		    UNION ALL
		    select

		        "from" AS address,
		        tr.contract_address AS token_address,
		        - value AS rawAmount
		    FROM
		        erc20. "ERC20_evt_Transfer" tr
		    WHERE evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		)
		, "historical_balances" as (
			select
				s.address as address
				, s.contract_address as token_address
				, s.rawAmount
		    from sandbox.token_balances_proposal_1 s
		    join "transfer_events" te
		    on te.address = s.address
		    and s.contract_address = te.token_address
		    where s.ts < hour_::timestamptz
		    )

		, "asset_union" as (
			select * from "transfer_events"
			union all
			select * from "historical_balances"

		)

		, "asset_balances" as (
		    select
		    	address,
		        token_address
		        , sum(rawAmount) AS rawAmount

		    FROM "asset_union" te
		    GROUP BY 1,2

		    )
		    ,
		    "asset_balance_readable" as (
		    select
		    hour_::timestamptz  as ts
		    , ab.address
		    , ab.token_address as contract_address
		    , tok."symbol" as token
		    , ab.rawAmount
		    , ab.rawAmount / 10^tok."decimals" as amount
		    , 0 as usd_amount
		    from "asset_balances" ab
		    left join  erc20."tokens" tok
		    on tok."contract_address" = ab."token_address"
		   	)
		   	select * from "asset_balance_readable"
		    where amount > 0.0001;
            -- to do -> add usd balance part, and check performance
            -- prepare views as separate files

   END LOOP;

RETURN r;
END
$function$;

INSERT INTO cron.job (schedule, command)
VALUES ('59 * * * *', $$SELECT token_balances.insert_addresses();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;