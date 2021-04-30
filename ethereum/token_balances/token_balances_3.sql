CREATE TABLE sandbox.token_balances_proposal_3(
   ts timestamptz,
   address bytea,
   token_balances jsonb,
   update_ts timestamptz,
   PRIMARY KEY( ts,address )

);



DO $$
DECLARE
   hour_   text;
   token_  text;
   arr varchar[];
BEGIN
  select array(select generate_series('2017-03-01', '2021-04-28', '1 hour'::interval)::timestamptz)
    into arr;

   for token_ in (select distinct symbol from erc20."tokens" where symbol in (
																			'BNT'
																			) )
   loop
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'Executing (% %)',token_, hour_;




	insert into vasa.token_balances_proposal_3
        -- select evt transfer events first and unify them

			with tkn_ as (select
		--'\xF4913E2952dA7202991e7c47D5A67A47AfF4C9FE'::bytea as bla,
		contract_address

		from erc20."tokens" where symbol = token_ )

		,
		 "transfer_events" as (
		    select
		        "to" AS address,
		        tr.contract_address AS token_address,
		        value AS rawAmount
		    from tkn_
		    join erc20. "ERC20_evt_Transfer" tr
		    on tkn_.contract_address = tr.contract_address
		    WHERE  evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		    --and "to" = '\xF4913E2952dA7202991e7c47D5A67A47AfF4C9FE'
		    UNION ALL
		    select

		        "from" AS address,
		        tr.contract_address AS token_address,
		        - value AS rawAmount
		    from tkn_
		    join erc20. "ERC20_evt_Transfer" tr
		    on tkn_.contract_address = tr.contract_address
		    WHERE evt_block_time >= hour_::timestamptz
		    and evt_block_time < hour_::timestamptz  + interval '1' hour
		    --and "from" = '\xF4913E2952dA7202991e7c47D5A67A47AfF4C9FE'
		)
		-- take historical balance and make sure that it is relevant to the current hour by joining on address and contract
		, "historical_balances_flattened" as (
			select * from
			vasa.token_balances_proposal_3 s,jsonb_to_recordset(s.token_balances) as items(amount numeric, symbol varchar,  "rawAmount" numeric, contract_address bytea)
		    --where symbol  = token_
--
			--			where s.ts =
--			hour_::timestamptz - interval '1' hour

		)
		,
		"historical_balances" as (
			select
				s.address as address
				, s.contract_address as token_address
				, s."rawAmount"
				, row_number () over (partition by  s.address, s.contract_address order by s.ts desc ) rn_
		    from "historical_balances_flattened" s
		    join "transfer_events" te
		    on te.address = s.address
		    and s.contract_address = te.token_address
		    where s.ts = hour_::timestamptz - interval '1' hour
		    )

		   , historical_balances_latest as (
		    select
		    address
			, token_address
			, "rawAmount" from historical_balances
		    where rn_ =1
		    )

        -- unify both current hourly balance and historical balance
		, "asset_union" as (
			select * from "transfer_events"
			union all
			select * from "historical_balances_latest"

		)
        -- calculate raw balance
		, "asset_balances" as (
		    select
		    	address,
		        token_address
		        , sum(te.rawAmount) AS rawAmount

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
        from "asset_balances" ab
        left join  erc20."tokens" tok
        on tok."contract_address" = ab."token_address"
        )
        -- filter for empty balances happens here, not sure how relevant is it, need to test when dealing with accuracy
        , data_from_that_hour as (
        select * from "asset_balance_readable"
        where amount > 0.0001)


        ,unified_copied_and_new as (
        select * from data_from_that_hour

        )

        select ts, address,  jsonb_agg ( json_build_object('contract_address',  contract_address, 'amount', amount, 'rawAmount',rawAmount, 'symbol', token))
        from unified_copied_and_new
       	group by 1 ,2
        ;


	end loop;
   END LOOP;
END $$;