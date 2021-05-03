CREATE
OR replace FUNCTION token_supply_over_time(from_ timestamptz, until_ timestamptz, token_ VARCHAR) RETURNS TABLE ( time_ timestamptz, balance NUMERIC ) language plpgsql AS $$
DECLARE -- variable declaration
BEGIN
   RETURN QUERY WITH hours AS
   (
      SELECT
         generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
   )
,
   token_balances_updated AS
   (
      SELECT
         ts,
         address,
         contract_address,
         token,
         amount,
         LEAD(ts, 1, now()) OVER (PARTITION BY contract_address, address
      ORDER BY
         ts) AS next_hour
      FROM
         vasa.token_balances
      WHERE
         token = token_
   )
,
   balance_all_days AS
   (
      SELECT
         d.hour_ AS time_,
         SUM(amount) AS balance
      FROM
         token_balances_updated b
         INNER JOIN
            hours d
            ON b.ts <= d.hour_
            AND d.hour_ < b.next_hour
      GROUP BY
         1
   )
   SELECT
      *
   FROM
      balance_all_days
   WHERE
      balance > 0.0001 ;
END
;
$$ -- token wallets in a point in time
CREATE
OR replace FUNCTION token_addresses_over_time(from_ timestamptz, until_ timestamptz, token_ VARCHAR) RETURNS TABLE ( time_ timestamptz, selected_token VARCHAR , wallet_address bytea ) language plpgsql AS $$
DECLARE -- variable declaration
BEGIN
   RETURN QUERY WITH hours AS
   (
      SELECT
         generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
   )
,
   token_balances_updated AS
   (
      SELECT
         ts,
         address,
         contract_address,
         token,
         amount,
         LEAD(ts, 1, now()) OVER (PARTITION BY contract_address, address
      ORDER BY
         ts) AS next_hour
      FROM
         vasa.token_balances
      WHERE
         token = token_
   )
,
   balance_all_days AS
   (
      SELECT
         d.hour_ AS time_,
         token,
         address
      FROM
         token_balances_updated b
         INNER JOIN
            hours d
            ON b.ts <= d.hour_
            AND d.hour_ < b.next_hour 				--group by 1,2
   )
   SELECT
      *
   FROM
      balance_all_days
   WHERE
      balance > 0.0001 ;
END
;
$$ CREATE
OR replace FUNCTION vasa.token_balance_per_address(from_ timestamptz, until_ timestamptz, address_ bytea) RETURNS TABLE ( time_ timestamptz, address__ bytea, token__ VARCHAR, balance__ NUMERIC ) language plpgsql AS $$
DECLARE -- variable declaration
BEGIN
   RETURN QUERY WITH hours AS
   (
      SELECT
         generate_series(from_::timestamptz, until_::timestamptz, '1 hour') AS hour_
   )
,
   token_balances_updated AS
   (
      SELECT
         ts,
         address,
         contract_address,
         symbol AS token,
         amount,
         LEAD(ts, 1, now()) OVER (PARTITION BY contract_address, address
      ORDER BY
         ts) AS next_hour
      FROM
         vasa.token_balances
      WHERE
         address = address_
   )
,
   balance_all_days AS
   (
      SELECT
         d.hour_ AS time_,
         address,
         token,
         SUM(amount) AS balance
      FROM
         token_balances_updated b
         INNER JOIN
            hours d
            ON b.ts <= d.hour_
            AND d.hour_ < b.next_hour
      GROUP BY
         1,
         2,
         3
   )
   SELECT
      *
   FROM
      balance_all_days
   WHERE
      balance > 0.0001 ;
END
;
$$ ;