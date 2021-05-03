CREATE TABLE vasa.token_balances( ts timestamptz, address bytea, contract_address bytea, token VARCHAR, rawAmount NUMERIC, amount NUMERIC, PRIMARY KEY( ts, address, contract_address ) );
CREATE INDEX
ON vasa.token_balances USING btree (address, contract_address);
CREATE INDEX
ON vasa.token_balances USING btree ( contract_address);
CREATE INDEX
ON vasa.token_balances USING btree ( ts);
CREATE
OR replace FUNCTION load_token_balances(until_ timestamptz , symbol_ VARCHAR) RETURNS timestamptz language plpgsql AS $$
DECLARE
BEGIN
   INSERT INTO
      vasa.token_balances 		-- select contact addresses from ecr20 table where metadata is stored
      WITH tkn_ AS
      (
         SELECT DISTINCT
            contract_address
         FROM
            erc20."tokens"
         WHERE
            symbol = symbol_
      )
,
      -- the code takes evt transfer events and combines them to get history of transaction from beginning of time until specified time period
      "transfer_events" AS
      (
         SELECT
            date_trunc('HOUR', evt_block_time ) AS ts,
            "TO" AS address,
            tr.contract_address AS token_address,
            VALUE AS rawAmount
         FROM
            tkn_
            JOIN
               erc20. "ERC20_evt_Transfer" tr
               ON tkn_.contract_address = tr.contract_address
         WHERE
            evt_block_time < until_::timestamptz
         UNION ALL
         SELECT
            date_trunc('HOUR', evt_block_time ) AS ts,
            "
         FROM
            " AS address,
            tr.contract_address AS token_address,
             - VALUE AS rawAmount
         FROM
            tkn_
            JOIN
               erc20. "ERC20_evt_Transfer" tr
               ON tkn_.contract_address = tr.contract_address
         WHERE
            evt_block_time < until_::timestamptz
      )
      -- calculate raw balance
,
      "asset_balances" AS
      (
         SELECT
            ts,
            address,
            token_address,
            SUM(rawAmount) AS rawAmount
         FROM
            "transfer_events" te
         GROUP BY
            1,
            2,
            3
      )
      --get rolling sum to have accurate hourly data
,
      "rolling_asset_balances_per_hour" AS
      (
         SELECT
            ts,
            address,
            token_address,
            SUM(rawAmount) OVER (PARTITION BY ts, address, token_address
         ORDER BY
            ts DESC ) AS rawAmount
         FROM
            "asset_balances"
      )
      -- make sure that the balance is converted from wei, and add token symbol
      -- join is done here to make the query more performant
,
      "asset_balance_readable" AS
      (
         SELECT
            ab.ts AS ts,
            ab.address,
            ab.token_address AS contract_address,
            tok."symbol" AS token,
            ab.rawAmount,
            ab.rawAmount / 10 ^ tok."decimals" AS amount
         FROM
            "rolling_asset_balances_per_hour" ab
            LEFT JOIN
               erc20."tokens" tok
               ON tok."contract_address" = ab."token_address"
      )
      SELECT
         *
      FROM
         "asset_balance_readable";
RETURN until_;
END
$$ ;


INSERT INTO
   cron.job (schedule, command)
VALUES
   (
      '0 * * * *',
      $$
      SELECT
         token_balances.insert_addresses(
         SELECT
            DATE_TRUNC('HOUR', now()), tnk.symbol )
         FROM
            (
               SELECT DISTINCT
                  SYMBOL
               FROM
                  "erc20".tokens
            )
            tnk;
 $$
   )
   ON CONFLICT (command) DO
   UPDATE
   SET
      schedule = EXCLUDED.schedule;