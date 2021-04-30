

CREATE TABLE sandbox.token_balances_proposal_2(
   ts timestamptz,
   address bytea,
   contract_address bytea,
   token varchar,
   rawAmount numeric,
   amount numeric,
   PRIMARY KEY( ts,address,contract_address )

);

CREATE INDEX ON sandbox.token_balances_proposal_2 USING btree (address, contract_address);
CREATE INDEX ON sandbox.token_balances_proposal_2 USING btree ( contract_address);
CREATE INDEX ON sandbox.token_balances_proposal_2 USING btree ( ts);

CREATE OR REPLACE FUNCTION token_balances.insert_hourly(start_ts, end_ts) RETURNS integer
LANGUAGE plpgsql AS $function$
--DO $$
DECLARE
   hour_   text;
   token_  text;
   arr varchar[];
BEGIN
  select array(select generate_series('2015-01-01', '2021-04-28', '1 hour'::interval)::timestamptz)
    into arr;

   for token_ in (select distinct symbol from erc20."tokens" where symbol in ('$DG',
																			'0xBTC',
																			'1INCH',
																			'1WO',
																			'4XB',
																			'AAVE',
																			'ABX',
																			'ABYSS',
																			'ACD',
																			'ACE') )
   loop
   FOREACH hour_  IN   array arr
   LOOP
      RAISE NOTICE 'Executing (% %)',token_, hour_;



	insert into vasa.token_balances_proposal_2
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
		    --and tr.contract_address = '\xEE06A81A695750E71A662B51066F2C74CF4478A0'
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
		    --and tr.contract_address = '\xEE06A81A695750E71A662B51066F2C74CF4478A0'
		    --and "from" = '\xF4913E2952dA7202991e7c47D5A67A47AfF4C9FE'
		)
		-- take historical balance and make sure that it is relevant to the current hour by joining on address and contract
		, "historical_balances" as (
			select
				s.address as address
				, s.contract_address as token_address
				, s.rawAmount
				, row_number () over (partition by  s.address, s.contract_address order by s.ts desc ) rn_
		    from "transfer_events" te
		    join vasa.token_balances_proposal_2 s
		    on te.address = s.address
		    and s.contract_address = te.token_address


		    )
		   , historical_balances_latest as (
		    select
		    address
			, token_address
			, rawAmount from historical_balances
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
        --, (ab.rawAmount / 10^tok."decimals")*p.price as usd_amount
        , hour_::timestamptz  as update_ts

        from "asset_balances" ab
        left join  erc20."tokens" tok
        on tok."contract_address" = ab."token_address"
            --commented out price calculation because it is heavy

--        LEFT JOIN  (
--                SELECT  date_trunc('hour', p.minute) as hour,
--                        contract_address,
--                        symbol,
--                        decimals,
--                        AVG(p.price) as price
--                FROM prices."usd" p
--                where p.minute >= hour_::timestamptz and p."minute" < hour_::timestamptz + interval '1' hour
--
--                GROUP BY 1, 2, 3, 4
--            ) p ON hour_::timestamptz = p.hour AND ab.token_address = p.contract_address
        )
        -- filter for empty balances happens here, not sure how relevant is it, need to test when dealing with accuracy
        , data_from_that_hour as (
        select * from "asset_balance_readable"
        where amount > 0.0001)


        select * from data_from_that_hour;

		end loop;
   END LOOP;
END $$;
RETURN hour_;
END
$function$;



INSERT INTO cron.job (schedule, command)
VALUES ('59 * * * *', $$SELECT token_balances.insert_addresses();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;





