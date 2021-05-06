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

