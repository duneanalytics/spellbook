CREATE TABLE IF NOT EXISTS hashflow.trades (
    composite_index integer,
    source text,
    block_time timestamptz NOT NULL,
    tx_hash bytea NOT NULL,
    fill_status boolean,
    method_id text,
    router_contract bytea,
    pool bytea,
    trader bytea,
    maker_token bytea,
    taker_token bytea,
    maker_symbol text,
    taker_symbol text,
    maker_token_amount float,
    taker_token_amount float,
    usd_amount float
);

CREATE OR REPLACE FUNCTION hashflow.insert_trades (start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

with event_decoded as (
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
                                                then 'BNB' else mp.symbol end as maker_symbol,
            case when quote->>'baseToken' = '0x0000000000000000000000000000000000000000'
                                            then 'BNB' else tp.symbol end as taker_symbol,
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
    join bsc.transactions tx on tx.hash = t.call_tx_hash
    left join event_decoded l on l.tx_id = ('\x' || substring(quote->>'txid' from 3))::bytea -- join on tx_id 1:1, no dup
    left join prices.usd tp on tp.minute = date_trunc('minute', t.call_block_time)
                                  and tp.contract_address = case when quote->>'baseToken' = '0x0000000000000000000000000000000000000000'
                                            then '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' else ('\x' || substring(quote->>'baseToken' from 3))::bytea end
                                            -- table has no BNB so using WBNB
    left join prices.usd mp on mp.minute = date_trunc('minute', t.call_block_time)
                                  and mp.contract_address = case when quote->>'quoteToken' = '0x0000000000000000000000000000000000000000'
                                            then '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' else ('\x' || substring(quote->>'quoteToken' from 3))::bytea end
                                            -- table has no BNB so using WBNB
    WHERE t.call_block_time >= start_ts AND t.call_block_time < end_ts
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
      FROM new_router
      ON CONFLICT DO NOTHING
      RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;

CREATE UNIQUE INDEX IF NOT EXISTS hashflow_trades_unique ON hashflow.trades (tx_hash, composite_index);
CREATE INDEX IF NOT EXISTS hashflow_trades_time_index ON hashflow.trades (block_time);

--backfill
SELECT hashflow.insert_trades('2022-01-24', (SELECT now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM hashflow.trades LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT hashflow.insert_trades((SELECT max(block_time) - interval '2 days' FROM hashflow.trades), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
