CREATE TABLE IF NOT EXISTS token_balances_v3(
   timestamp timestamptz,
   wallet_address bytea,
   token_address bytea,
   token_symbol text,
   amount_raw NUMERIC,
   amount NUMERIC,
   PRIMARY KEY(timestamp, wallet_address, token_address)
);

CREATE INDEX IF NOT EXISTS token_balances_v3_wallet_address_token_address_idx ON token_balances_v3 USING btree (wallet_address, token_address);
CREATE INDEX IF NOT EXISTS token_balances_v3_wallet_address_token_address_timestamp_desc_idx ON token_balances_v3 USING btree (wallet_address, token_address, timestamp DESC) include (amount);
CREATE INDEX IF NOT EXISTS token_balances_v3_timestamp_idx ON token_balances_v3 USING btree(timestamp);

CREATE OR replace FUNCTION load_token_balances_v3(to_time_ timestamptz) RETURNS void language plpgsql AS 
$$ DECLARE 
_row_count integer;
_last_insert_time timestamptz;
BEGIN
-- add check for last block time
_last_insert_time := coalesce((select timestamp from token_balances_v3 order by timestamp desc limit 1), '2015-01-01 00:00:00');
RAISE NOTICE 'LAST INSERT %', _last_insert_time;
insert into
   token_balances_v3 with transfers AS (
    SELECT
        date_trunc('hour', v.ts) block_hour,
        v.wallet_address,
        v.token_address,
        sum(v.amount) as amount
    FROM
        erc20."ERC20_evt_Transfer" t
    cross join lateral (VALUES
        (evt_block_time, "from", contract_address, - value),
        (evt_block_time, "to", contract_address, value)
    ) v(ts, wallet_address, token_address, amount)
    WHERE
        evt_block_time >= _last_insert_time + interval '1 hour'
        AND evt_block_time < to_time_
    GROUP BY 1, 2, 3
        UNION ALL(
        -- For every wallet address get its latest balance
        SELECT DISTINCT ON (wallet_address, token_address)
            timestamp,
            wallet_address,
            token_address,
            amount_raw
        FROM token_balances_v3
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
