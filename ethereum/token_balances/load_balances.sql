CREATE TABLE IF NOT EXISTS token_balances(
   timestamp timestamptz,
   wallet_address bytea,
   token_address bytea,
   token_symbol text,
   amount_raw NUMERIC,
   amount NUMERIC,
   PRIMARY KEY(timestamp, wallet_address, token_address)
);

CREATE INDEX IF NOT EXISTS token_balances_wallet_address_token_address_idx ON token_balances USING btree (wallet_address, token_address);
CREATE INDEX IF NOT EXISTS token_balances_wallet_address_token_address_timestamp_idx ON token_balances USING btree (wallet_address, token_address, timestamp) include (amount);
CREATE INDEX IF NOT EXISTS token_balances_token_timestamp_idx ON token_balances USING btree (timestamp, token_address);

CREATE OR replace FUNCTION load_token_balances(contract_address_ bytea, to_time_ timestamptz) RETURNS void language plpgsql AS 
$$ DECLARE 
_row_count integer;
_last_insert_time timestamptz;
BEGIN
_last_insert_time := coalesce((select timestamp from token_balances where token_address = contract_address_ order by timestamp desc limit 1), '2015-01-01 00:00:00');
RAISE NOTICE 'LAST INSERT %', _last_insert_time;
insert into
   token_balances with transfers AS (
      SELECT
         date_trunc('hour', evt_block_time) block_hour,
         wallet_address,
         token_address,
         sum(amount) as amount
      FROM
         (
            SELECT
               evt_block_time,
               "to" AS wallet_address,
               contract_address AS token_address,
               value AS amount
            FROM
               erc20."ERC20_evt_Transfer"
            WHERE
               contract_address = contract_address_
               AND evt_block_time >= _last_insert_time + interval '1 hour'
               AND evt_block_time < to_time_
            UNION
            ALL
            SELECT
               evt_block_time,
               "from" AS wallet_address,
               contract_address AS token_address,
               - VALUE AS amount
            FROM
               erc20."ERC20_evt_Transfer"
            WHERE
               contract_address = contract_address_
               AND evt_block_time >= _last_insert_time + interval '1 hour'
               AND evt_block_time < to_time_
         ) transfers
      GROUP BY 1, 2, 3
      UNION ALL(
      -- For every wallet address get its latest balance
      SELECT DISTINCT ON (wallet_address, token_address)
         timestamp,
         wallet_address,
         token_address,
         amount_raw
      FROM token_balances 
      WHERE 
         token_address = contract_address_ 
         and timestamp < to_time_
      ORDER BY wallet_address, token_address, timestamp desc
      )
    ),
   wallet_balances AS (
      SELECT
         block_hour,
         wallet_address,
         token_address,
         SUM(amount) OVER (
            PARTITION BY wallet_address,
            token_address
            ORDER BY
               block_hour
         ) as amount
      FROM
         transfers
   )
SELECT
   wb.block_hour as timestamp,
   wb.wallet_address,
   wb.token_address,
   tokens.symbol AS token_symbol,
   wb.amount AS amount_raw,
   wb.amount / 10 ^ tokens.decimals AS amount
FROM
   wallet_balances wb
   LEFT JOIN erc20.tokens tokens ON tokens.contract_address = wb.token_address
WHERE 
   -- don't insert rows for timeblocks that already exist
   wb.block_hour > _last_insert_time
ORDER BY
   1 DESC;
GET DIAGNOSTICS _row_count = ROW_COUNT;
  RAISE NOTICE 'INSERTED %', _row_count;
END $$;

CREATE OR replace FUNCTION load_token_balances_batch_excluding_usdt(limit_ int) RETURNS void language plpgsql AS $$ DECLARE BEGIN
   perform load_token_balances(contract_address, date_trunc('hour', now() - interval '1 hour')) 
   from erc20.tokens 
   where symbol not in ('USDC', 'WETH', 'USDT') and contract_address not in (select distinct token_address from token_balances)
   order by symbol, contract_address 
   limit limit_;
END $$;

-- these are too big to do in one go, must be handled separately
select load_token_balances('\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea, date_trunc('hour', now() - interval '1 hour')); -- usdc
select load_token_balances('\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea, date_trunc('hour', now() - interval '1 hour')); -- weth
select load_token_balances('\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea, date_trunc('hour', now() - interval '1 hour')); -- usdt

-- For everything else do in batches of 100
-- There are 800~ erc20.tokens 
select load_token_balances_batch_excluding_usdt(100); -- 1
select load_token_balances_batch_excluding_usdt(100); -- 2
select load_token_balances_batch_excluding_usdt(100); -- 3
select load_token_balances_batch_excluding_usdt(100); -- 4
select load_token_balances_batch_excluding_usdt(100); -- 5
select load_token_balances_batch_excluding_usdt(100); -- 6
select load_token_balances_batch_excluding_usdt(100); -- 7
select load_token_balances_batch_excluding_usdt(100); -- 8
select load_token_balances_batch_excluding_usdt(100); -- 9


