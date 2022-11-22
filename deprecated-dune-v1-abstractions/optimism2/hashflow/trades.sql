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

with new_pool as (
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
                    l."baseTokenAmount"/power(10, tp.decimals) * tp.median_price,
                    l."quoteTokenAmount"/power(10, mp.decimals) * mp.median_price)
                    as usd_amount

    from hashflow."Pool_evt_Trade" l 
    join optimism.transactions tx on tx.hash = l.evt_tx_hash
        and tx.block_time >= start_ts 
        AND tx.block_time < end_ts
    left join prices.approx_prices_from_dex_data tp on tp.hour = date_trunc('hour', tx.block_time)
                                  and tp.contract_address = case when l."baseToken" = '\x0000000000000000000000000000000000000000'
                                            then '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' else l."baseToken" end
                                  AND tp.hour >= start_ts
                                  AND tp.hour < end_ts
    left join prices.approx_prices_from_dex_data mp on mp.hour = date_trunc('hour', tx.block_time)
                                  and mp.contract_address = case when l."quoteToken" = '\x0000000000000000000000000000000000000000'
                                            then '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' else l."quoteToken" end
                                  AND tp.hour >= start_ts
                                  AND tp.hour < end_ts
    WHERE l.evt_block_time >= start_ts 
            AND l.evt_block_time < end_ts
)
, all_trades as (
    
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
SELECT hashflow.insert_trades('2022-05-07', (SELECT now() - interval '20 minutes')) WHERE NOT EXISTS (SELECT * FROM hashflow.trades LIMIT 1);

INSERT INTO cron.job (schedule, command)
VALUES ('15 * * * *', $$SELECT hashflow.insert_trades((SELECT max(block_time) - interval '2 days' FROM hashflow.trades), (SELECT now() - interval '20 minutes'));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;