CREATE TABLE IF NOT EXISTS hashflow.trades (
	composite_index int4 NULL,
	"source" text NULL,
	block_time timestamptz NOT NULL,
	tx_hash bytea NOT NULL,
	fill_status bool NULL,
	method_id text NULL,
	router_contract bytea NULL,
	pool bytea NULL,
	trader bytea NULL,
	maker_token bytea NULL,
	taker_token bytea NULL,
	maker_symbol text NULL,
	taker_symbol text NULL,
	maker_token_amount float8 NULL,
	taker_token_amount float8 NULL,
	usd_amount float8 NULL
);

CREATE OR REPLACE FUNCTION hashflow.insert_trades(start_ts timestamp with time zone, end_ts timestamp with time zone DEFAULT now())
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE r integer;
BEGIN

with legacy_routers as (
    select
            t.block_time,
            t.tx_hash,
            error is null as fill_status,
            substring(input,1,4)::text as method_id,
            "to" as router_contract,
            substring(input, 17, 20) as pool, --mm
            substring(input, 49, 20) as trader, --trader
            (case when substring(input,1,4) = '\xc7f6b19d'
                    then substring(input, 81, 20)
                else '\x0000000000000000000000000000000000000000'::bytea end) as maker_token,
            (case when substring(input,1,4) = '\xc7f6b19d'
                    then '\x0000000000000000000000000000000000000000'::bytea
                else substring(input, 81, 20) end) as taker_token, --eth
            (case when substring(input,1,4) = '\xc7f6b19d'
                    then e.symbol
                else 'ETH' end) as maker_symbol,
            (case when substring(input,1,4) = '\xc7f6b19d'
                    then 'ETH'
                else e.symbol end) as taker_symbol,
            (case when substring(input,1,4) = '\xc7f6b19d'
                    then bytea2numericpy(substring(input, 145, 20))/power(10,e.decimals)
                else bytea2numericpy(substring(input, 145, 20))/1e18 end) as maker_token_amount
            , (case when substring(input,1,4) = '\xc7f6b19d'
                    then bytea2numericpy(substring(input, 113, 20))/1e18
                else bytea2numericpy(substring(input, 113, 20))/power(10,e.decimals) end) as taker_token_amount
            , (case when substring(input,1,4) = '\xc7f6b19d'
                    then bytea2numericpy(substring(input, 113, 20))/1e18 * price
                else bytea2numericpy(substring(input, 145, 20))/1e18 * price end) as usd_amount
    from ethereum.traces t
    left join prices.usd p on minute = date_trunc('minute', t.block_time)
                            AND p.minute >= start_ts
                            AND p.minute < end_ts
    left join erc20.tokens e on e.contract_address = substring(input, 81, 20)
    where  trace_address::text = '{}'  --top level call
        and "to" in ('\x9d4fc735e1a596420d24a266b7b5402fe4ec153c', '\x2405cb057a9baf85daa11ce9832baed839b6871c')
        and substring(input,1,4) in ('\x9ec7605b',  --tokenToEth
                                        '\xc7f6b19d') --ethToToken
        and p.symbol='WETH'
        AND t.block_time >= start_ts AND t.block_time < end_ts

    union all

    select
            t.block_time,
            t.tx_hash,
            error is null as fill_status,
            substring(input,1,4)::text as method_id,
            "to" as router_contract,
            substring(input, 17, 20) as pool, --mm
            substring(input, 49, 20) as trader, --trader
            substring(input, 113, 20) as maker_token,
            substring(input, 81, 20) as taker_token,
            mp.symbol as maker_symbol,
            tp.symbol as taker_symbol,
            bytea2numericpy(substring(input, 177, 20))/power(10,mp.decimals)  as maker_token_amount,
            bytea2numericpy(substring(input, 145, 20))/power(10,tp.decimals)  as taker_token_amount,
            coalesce(
                bytea2numericpy(substring(input, 145, 20))/power(10, tp.decimals) * tp.price,
                bytea2numericpy(substring(input, 177, 20))/power(10, mp.decimals) * mp.price) as usd_amount
    from ethereum.traces t
    left join prices.usd tp on tp.minute = date_trunc('minute', t.block_time) and tp.contract_address = substring(input, 81, 20)
                            AND tp.minute >= start_ts
                            AND tp.minute < end_ts
    left join prices.usd mp on mp.minute = date_trunc('minute', t.block_time) and mp.contract_address = substring(input, 113, 20)
                            AND mp.minute >= start_ts
                            AND mp.minute < end_ts
    where  trace_address::text = '{}'  --top level call
        and "to" in ('\x455a3B3Be6e7C8843f2b03A1cA22A5a5727ef5C4','\x9d4fc735e1a596420d24a266b7b5402fe4ec153c', '\x2405cb057a9baf85daa11ce9832baed839b6871c','\x043389f397ad72619d05946f5f35426a7ace6613')
        and substring(input,1,4) in ('\x064f0410','\x4d0246ad') --tokenToToken
        AND t.block_time >= start_ts AND t.block_time < end_ts


    union all

    select
            t.block_time,
            t.tx_hash,
            error is null as fill_status,
            substring(input,1,4)::text as method_id,
            "to" as router_contract,
            substring(input, 17, 20) as pool, --mm
            substring(input, 49, 20) as trader, --trader
            (case when substring(input,1,4) = '\xe43d9733'
                    then substring(input, 81, 20)
                else '\x0000000000000000000000000000000000000000'::bytea end) as maker_token,
            (case when substring(input,1,4) = '\xe43d9733'
                    then '\x0000000000000000000000000000000000000000'::bytea
                else substring(input, 81, 20) end) as taker_token, --eth
            (case when substring(input,1,4) = '\xe43d9733'
                    then e.symbol
                else 'ETH' end) as maker_symbol,
            (case when substring(input,1,4) = '\xe43d9733'
                    then 'ETH'
                else e.symbol end) as taker_symbol,
            (case when substring(input,1,4) = '\xe43d9733'
                    then bytea2numericpy(substring(input, 145, 20))/power(10,e.decimals)
                else bytea2numericpy(substring(input, 145, 20))/1e18 end) as maker_token_amount
            , (case when substring(input,1,4) = '\xe43d9733'
                    then bytea2numericpy(substring(input, 113, 20))/1e18
                else bytea2numericpy(substring(input, 113, 20))/power(10,e.decimals) end) as taker_token_amount
            , (case when substring(input,1,4) = '\xe43d9733'
                    then bytea2numericpy(substring(input, 113, 20))/1e18 * price
                else bytea2numericpy(substring(input, 145, 20))/1e18 * price end) as usd_amount
        from ethereum.traces t
        left join prices.usd p on minute = date_trunc('minute', t.block_time)
                                AND p.minute >= start_ts AND p.minute < end_ts
        left join erc20.tokens e on e.contract_address = substring(input, 81, 20)
        where  trace_address::text = '{}'  --top level call
            and "to" in ('\x455a3B3Be6e7C8843f2b03A1cA22A5a5727ef5C4','\x043389f397ad72619d05946f5f35426a7ace6613')
            and substring(input,1,4) in ('\xd0529c02',  --tokenToEth
                                        '\xe43d9733') --ethToToken
            and p.symbol='WETH'

            AND t.block_time >= start_ts AND t.block_time < end_ts

), event_decoding_legacy_router as (
  select      l.tx_hash,
              l.index as evt_index,
              substring(l.data, 13, 20) as trader,
              substring(l.data,33,32) as tx_id,
              substring(l.data, 109, 20) as maker_token,
              substring(l.data, 77, 20) as taker_token,
              bytea2numericpy(substring(l.data, 173, 20)) as maker_token_amount,
              bytea2numericpy(substring(l.data, 141, 20)) as taker_token_amount
   from ethereum.logs l
      where
        topic1 ='\x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e'
        -- Trade0()
        AND block_number <= 13974528 -- block of last trade of all legacy routers
        AND block_time >= start_ts AND block_time < end_ts

  union all

  select      l.tx_hash,
              l.index as evt_index,
              substring(l.data, 45, 20) as trader,
              substring(l.data,65,32) as tx_id,
              substring(l.data, 141, 20) as maker_token,
              substring(l.data, 109, 20) as taker_token,
              bytea2numericpy(substring(l.data, 205, 20)) as maker_token_amount,
              bytea2numericpy(substring(l.data, 173, 20)) as taker_token_amount
   from ethereum.logs l
      where
        topic1 ='\xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5'
        -- Trade()
        AND block_number <= 13974528 -- block of last trade of all legacy routers
        AND block_time >= start_ts AND block_time < end_ts

), legacy_router_w_integration as (
    select  coalesce(l.evt_index,-1)::int as composite_index,
            substring(input, 324, 1)::text as source,
            t.block_time,
            t.tx_hash,
            t.error is null as fill_status,
            substring(t.input,1,4)::text as method_id,
            t."to" as router_contract,
            substring(t.input, 17, 20) as pool, --mm
            tx."from" as trader,
            -- adjusted to use tx sender due to integration
            -- substring(t.input, 49, 20) as trader, --trader
            maker_token,
            taker_token,
            case when substring(input, 113, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then 'ETH' else mp.symbol end as maker_symbol,
            case when substring(input, 81, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then 'ETH' else tp.symbol end as taker_symbol,
            case when l.tx_hash is not null then maker_token_amount/power(10,mp.decimals) else null end  as maker_token_amount,
            case when l.tx_hash is not null then taker_token_amount/power(10,tp.decimals) else null end  as taker_token_amount,
            case when l.tx_hash is not null then
                coalesce(
                    taker_token_amount/power(10, tp.decimals) * tp.price,
                    maker_token_amount/power(10, mp.decimals) * mp.price) else null end as usd_amount
    from ethereum.traces t
    left join ethereum.transactions tx on tx.hash = t.tx_hash
        and tx.block_time >= start_ts 
        and tx.block_time < end_ts
    left join event_decoding_legacy_router l on l.tx_id = substring(t.input,325,32) -- join on tx_id 1:1, no dup
    left join prices.usd tp on tp.minute = date_trunc('minute', t.block_time)
                                  and tp.contract_address = case when substring(input, 81, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea else substring(input, 81, 20) end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    left join prices.usd mp on mp.minute = date_trunc('minute', t.block_time)
                                  and mp.contract_address = case when substring(input, 113, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea else substring(input, 113, 20) end
                                  AND mp.minute >= start_ts
                                  AND mp.minute < end_ts
    where
        -- trace_address::text = '{}'  --top level call -- removed this because of 1inch integration
        t."to" in ('\xa18607ca4a3804cc3cd5730eafefcc47a7641643')
        and substring(input,1,4) in ('\xba93c39c') -- swap()
        AND t.block_time >= start_ts AND t.block_time < end_ts
        AND t.block_number <= 13803909 -- block of last trade of this legacy router

    union all

    select  coalesce(l.evt_index,-1)::int as composite_index,
            substring(input, 484, 1)::text as source,
            t.block_time,
            t.tx_hash,
            t.error is null as fill_status,
            'tradeSingleHop' as method_id,
            t."to" as router_contract,
            substring(t.input, 49, 20) as pool, --mm
            tx."from" as trader,
            -- adjusted to use tx sender due to integration
            -- substring(t.input, 49, 20) as trader, --trader
            maker_token,
            taker_token,
            case when substring(input, 209, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then 'ETH' else mp.symbol end as maker_symbol,
            case when substring(input, 177, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then 'ETH' else tp.symbol end as taker_symbol,
            case when l.tx_hash is not null then maker_token_amount/power(10,mp.decimals) else null end  as maker_token_amount,
            case when l.tx_hash is not null then taker_token_amount/power(10,tp.decimals) else null end  as taker_token_amount,
            case when l.tx_hash is not null then
                coalesce(
                    taker_token_amount/power(10, tp.decimals) * tp.price,
                    maker_token_amount/power(10, mp.decimals) * mp.price) else null end as usd_amount
    from ethereum.traces t
    left join ethereum.transactions tx on tx.hash = t.tx_hash
        and tx.block_time >= start_ts 
        and tx.block_time < end_ts
    left join event_decoding_legacy_router l on l.tx_id = substring(t.input,485,32) -- join on tx_id 1:1, no dup
    left join prices.usd tp on tp.minute = date_trunc('minute', t.block_time)
                                  and tp.contract_address = case when substring(input, 177, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea else substring(input, 177, 20) end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    left join prices.usd mp on mp.minute = date_trunc('minute', t.block_time)
                                  and mp.contract_address = case when substring(input, 209, 20) = '\x0000000000000000000000000000000000000000'::bytea
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea else substring(input, 209, 20) end
                                  AND mp.minute >= start_ts
                                  AND mp.minute < end_ts
    where
        -- trace_address::text = '{}'  --top level call -- removed this because of 1inch integration
        t."to" in ('\x6ad3dac99c9a4a480748c566ce7b3503506e3d71')
        and substring(input,1,4) in ('\xf0910b2b') -- tradeSingleHop()
        AND t.block_time >= start_ts AND t.block_time < end_ts
        AND t.block_number <= 13974528 -- block of last trade of this legacy router

), event_decoded as (
    select      evt_tx_hash as tx_hash,
                evt_index,
                trader,
                txid as tx_id,
                "quoteToken" as maker_token,
                "baseToken" as taker_token,
                "quoteTokenAmount" as maker_token_amount,
                "baseTokenAmount" as taker_token_amount
     from hashflow."Pool_evt_Trade0"
          WHERE evt_block_time >= start_ts AND evt_block_time < end_ts

    union all

    select      evt_tx_hash as tx_hash,
                evt_index,
                trader,
                txid as tx_id,
                "quoteToken" as maker_token,
                "baseToken" as taker_token,
                "quoteTokenAmount" as maker_token_amount,
                "baseTokenAmount" as taker_token_amount
     from hashflow."Pool_evt_Trade"
          WHERE evt_block_time >= start_ts AND evt_block_time < end_ts

) , new_router as (

    select  coalesce(l.evt_index,-1)::int as composite_index,
            (quote->>'flag')::text as source,
            t.call_block_time as block_time,
            t.call_tx_hash as tx_hash,
            t.call_success as fill_status,
            'tradeSingleHop' as method_id,
            t.contract_address as router_contract,
            ('\x' || substring(quote->>'pool' from 3))::bytea as pool,
            tx."from" as trader,
            ('\x' || substring(quote->>'quoteToken' from 3))::bytea as maker_token,
            ('\x' || substring(quote->>'baseToken' from 3))::bytea as taker_token,
            case when quote->>'quoteToken' = '0x0000000000000000000000000000000000000000'
                                                then 'ETH' else mp.symbol end as maker_symbol,
            case when quote->>'baseToken' = '0x0000000000000000000000000000000000000000'
                                            then 'ETH' else tp.symbol end as taker_symbol,
            case when l.tx_hash is not null then maker_token_amount/power(10,mp.decimals)
                                            else (quote->'maxQuoteTokenAmount')::float/power(10,mp.decimals)
                                            end  as maker_token_amount,
            case when l.tx_hash is not null then taker_token_amount/power(10,tp.decimals)
                                            else (quote->'maxBaseTokenAmount')::float/power(10,tp.decimals)
                                            end  as taker_token_amount,
            case when l.tx_hash is not null then
                        coalesce(
                            taker_token_amount/power(10, tp.decimals) * tp.price,
                            maker_token_amount/power(10, mp.decimals) * mp.price)
                    else coalesce(
                            (quote->'maxBaseTokenAmount')::float/power(10, tp.decimals) * tp.price,
                            (quote->'maxQuoteTokenAmount')::float/power(10, mp.decimals) * mp.price)
                    end as usd_amount

    from hashflow."Router_call_tradeSingleHop" t 
            -- 2022-01-10 to 2022-04-08
    join ethereum.transactions tx on tx.hash = t.call_tx_hash
        and tx.block_time >= start_ts 
        and tx.block_time < end_ts
    left join event_decoded l on l.tx_id = ('\x' || substring(quote->>'txid' from 3))::bytea -- join on tx_id 1:1, no dup
    left join prices.usd tp on tp.minute = date_trunc('minute', t.call_block_time)
                                  and tp.contract_address = case when quote->>'baseToken' = '0x0000000000000000000000000000000000000000'
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else ('\x' || substring(quote->>'baseToken' from 3))::bytea end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    left join prices.usd mp on mp.minute = date_trunc('minute', t.call_block_time)
                                  and mp.contract_address = case when quote->>'quoteToken' = '0x0000000000000000000000000000000000000000'
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else ('\x' || substring(quote->>'quoteToken' from 3))::bytea end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    WHERE t.call_block_time >= start_ts 
            AND t.call_block_time < end_ts
), new_pool as (
    -- subquery for including new pools created on 2022-04-09
    -- same Trade event abi, effectively only from table hashflow.Pool_evt_Trade since 2022-04-09
    select  l.evt_index as composite_index,
            null as source, -- no join on call for this batch, refer to metabase for source info
            tx.block_time as block_time,
            tx.hash as tx_hash,
            TRUE as fill_status, -- without call we are only logging successful fills
            null as method_id, -- without call we dont have function call info
            tx."to" as router_contract, -- taking top level contract called in tx as router, not necessarily HF contract
            l."pool" as pool,
            tx."from" as trader,
            l."quoteToken" as maker_token,
            l."baseToken" as taker_token,
            case when l."quoteToken" = '\x0000000000000000000000000000000000000000'
                                            then 'ETH' else mp.symbol end as maker_symbol,
            case when l."baseToken" = '\x0000000000000000000000000000000000000000'
                                            then 'ETH' else tp.symbol end as taker_symbol,
            l."quoteTokenAmount"/power(10,mp.decimals) as maker_token_amount,
            l."baseTokenAmount"/power(10,tp.decimals) as taker_token_amount,
            coalesce(
                    l."baseTokenAmount"/power(10, tp.decimals) * tp.price,
                    l."quoteTokenAmount"/power(10, mp.decimals) * mp.price)
                    as usd_amount

    from hashflow."Pool_evt_Trade" l 
    join ethereum.transactions tx on tx.hash = l.evt_tx_hash
        and tx.block_time >= start_ts 
        and tx.block_time < end_ts
    left join prices.usd tp on tp.minute = date_trunc('minute', tx.block_time)
                                  and tp.contract_address = case when l."baseToken" = '\x0000000000000000000000000000000000000000'
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else l."baseToken" end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    left join prices.usd mp on mp.minute = date_trunc('minute', tx.block_time)
                                  and mp.contract_address = case when l."quoteToken" = '\x0000000000000000000000000000000000000000'
                                            then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else l."quoteToken" end
                                  AND tp.minute >= start_ts
                                  AND tp.minute < end_ts
    WHERE l.evt_block_time > '2022-04-08' -- necessary filter to only include new trades
            AND l.evt_block_time >= start_ts 
            AND l.evt_block_time < end_ts
)
, all_trades as (
    select
          -1::int as composite_index,
          -- was decoding from trace, no log_index, only single swap exist so works as PK
          '\x00'::text as source,
          -- all from native front end, no integration yet
          *
    from legacy_routers
    union all
    select * from legacy_router_w_integration
    union all
    select * from new_router
    union all
    select * from new_pool

), rows AS (
      INSERT INTO hashflow.trades (
          composite_index,
          source,
          block_time,
          tx_hash,
          fill_status,
          method_id,
          router_contract,
          pool,
          trader,
          maker_token,
          taker_token,
          maker_symbol,
          taker_symbol,
          maker_token_amount,
          taker_token_amount,
          usd_amount
      )
      SELECT
          composite_index,
          source,
          block_time,
          tx_hash,
          fill_status,
          method_id,
          router_contract,
          pool,
          trader,
          maker_token,
          taker_token,
          maker_symbol,
          taker_symbol,
          maker_token_amount,
          taker_token_amount,
          usd_amount
      FROM all_trades
      ON CONFLICT DO NOTHING
      RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$
;

CREATE INDEX IF NOT EXISTS hashflow_trades_time_index ON hashflow.trades USING btree (block_time);
CREATE UNIQUE INDEX IF NOT EXISTS hashflow_trades_unique ON hashflow.trades USING btree (tx_hash, composite_index);

--backfill
delete FROM hashflow.trades;
SELECT hashflow.insert_trades('2021-04-28', (SELECT now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM hashflow.trades LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT hashflow.insert_trades((SELECT max(block_time) - interval '2 days' FROM hashflow.trades), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;