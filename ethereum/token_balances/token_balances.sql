

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


CREATE OR REPLACE FUNCTION token_balances.insert_hourly(start_ts, end_ts) RETURNS integer
LANGUAGE plpgsql AS $function$
--DO $$
DECLARE
   hour_   text;
   arr varchar[];
BEGIN
  select array(select generate_series(start_ts, end_ts, '1 hour'::interval)::timestamptz)
    into arr;
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'Executing (%)',hour_;

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
		    where s.ts = hour_::timestamptz - interval '1' hour
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
RETURN hour_;
END
$function$;



INSERT INTO cron.job (schedule, command)
VALUES ('59 * * * *', $$SELECT token_balances.insert_addresses();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;





--- version that creates balance for each hour


CREATE TABLE sandbox.token_balances_proposal_2(
   ts timestamptz,
   address bytea,
   contract_address bytea,
   token varchar,
   rawAmount numeric,
   amount numeric,
   usd_amount numeric,
   update_ts timestamptz,
   PRIMARY KEY( ts,address,contract_address )

);

CREATE INDEX ON sandbox.token_balances_proposal_2 USING btree (address, contract_address);
CREATE INDEX ON sandbox.token_balances_proposal_2 USING btree ( contract_address);

DO $$
DECLARE
   hour_   text;
   arr varchar[];
BEGIN
  select array(select generate_series('2021-03-21', '2021-03-23', '1 hour'::interval)::timestamptz)
    into arr;
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'Executing (%)',hour_;

	insert into sandbox.token_balances_proposal_2
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
		    from sandbox.token_balances_proposal_2 s
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
        , hour_::timestamptz  as update_ts
        from "asset_balances" ab
        left join  erc20."tokens" tok
        on tok."contract_address" = ab."token_address"
        )
        -- filter for empty balances happens here, not sure how relevant is it, need to test when dealing with accuracy
        , data_from_that_hour as (
        select * from "asset_balance_readable"
        where amount > 0.0001)

        , historical_data_updated as (
        select hour_::timestamptz  as ts, abr.address, abr.contract_address, abr.token, abr.rawAmount, abr.amount, abr.usd_amount, abr.update_ts
        from sandbox.token_balances_proposal_2 tbp
        left join "data_from_that_hour" abr
        on abr.address = tbp.address
        and abr.contract_address = tbp.contract_address
        where abr.address is null and abr.contract_address is null
        and tbp.ts < hour_::timestamptz

        )
        ,unified_copied_and_new as (
        select * from data_from_that_hour
        union all
        select * from historical_data_updated tt
        where tt.address is not null )

        select * from unified_copied_and_new;

        -- to do -> add usd balance part, and check performance, runs 30s per day for now, but needs load
        -- prepare views as separate files

   END LOOP;
END $$;




DO $$
DECLARE
   hour_   text;
   arr varchar[];
BEGIN
  select array(select generate_series('2021-03-21', '2021-03-23', '1 hour'::interval)::timestamptz)
    into arr;
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'Executing (%)',hour_;

	insert into sandbox.token_balances_proposal_3
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
		, "historical_balances_flattened" as (
			select address, contract_address , rawAmount from sandbox.token_balances_proposal_3 s,jsonb_to_recordset(s.token_balances) as items(token text, rawAmount numeric, contract_address text)
			where s.ts = hour_::timestamptz - interval '1' hour

		)

		"historical_balances" as (
			select
				s.address as address
				, s.contract_address as token_address
				, s.rawAmount
		    from "historical_balances_flattened" s
		    join "transfer_events" te
		    on te.address = s.address
		    and s.contract_address = te.token_address
		    where s.ts = hour_::timestamptz - interval '1' hour
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
        , hour_::timestamptz  as update_ts
        from "asset_balances" ab
        left join  erc20."tokens" tok
        on tok."contract_address" = ab."token_address"
        )
        -- filter for empty balances happens here, not sure how relevant is it, need to test when dealing with accuracy
        , data_from_that_hour as (
        select * from "asset_balance_readable"
        where amount > 0.0001)

        , historical_data_updated as (
        select hour_::timestamptz  as ts, abr.address, abr.contract_address, abr.token, abr.rawAmount, abr.amount, abr.usd_amount, abr.update_ts
        from sandbox.token_balances_proposal_3 tbp
        left join "data_from_that_hour" abr
        on abr.address = tbp.address
        and abr.contract_address = tbp.contract_address
        where abr.address is null and abr.contract_address is null
        and tbp.ts < hour_::timestamptz

        )
        ,unified_copied_and_new as (
        select * from data_from_that_hour
        union all
        select * from historical_data_updated tt
        where tt.address is not null )

        select ts, address,  jsonb_agg ( json_build_object('token', json_build_object('contract_address',  contract_address, 'amount', amount, 'rawAmount',rawAmount)))
        from unified_copied_and_new
       	group by 1 ,2
        ;

   END LOOP;
END $$;