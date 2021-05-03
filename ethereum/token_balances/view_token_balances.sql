-- view to query balances over time per hour
CREATE
OR replace view vasa.balances_per_hour AS WITH hours AS
(
   SELECT
      generate_series('2015-07-01'::TIMESTAMP, date_trunc('hour', NOW()), '1 hour') AS hour_ 		-- Generate all days since the first contract
)
,
token_balances_updated AS
(
   SELECT
      ts,
      address,
      contract_address,
      token,
      amount balance,
      LEAD(ts, 1, now()) OVER (PARTITION BY address
   ORDER BY
      ts) AS next_hour
   FROM
      vasa.token_balances
)
,
balance_all_days AS
(
   SELECT
      d.hour_ ts,
      address,
      b.token::text AS symbol,
      b.contract_address,
      balance,
      b.next_hour
   FROM
      token_balances_updated b
      INNER JOIN
         hours d
         ON b.ts <= d.hour_
         AND d.hour_ < b.next_hour 			-- Yields an observation for every day after the first transfer until the next day with transfer
)
SELECT
   *
FROM
   balance_all_days
WHERE
   balance > 0.0001 ;
-- balance per token per hour
CREATE
OR replace view vasa.supply_per_token_per_hour AS WITH hours AS
(
   SELECT
      generate_series('2015-07-01'::TIMESTAMP, date_trunc('hour', NOW()), '1 hour') AS hour_ 		-- Generate all days since the first contract
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
)
,
balance_all_days AS
(
   SELECT
      d.hour_ ts,
      token,
      SUM(amount) AS balance
   FROM
      token_balances_updated b
      INNER JOIN
         hours d
         ON b.ts <= d.hour_
         AND d.hour_ < b.next_hour 			-- Yields an observation for every day after the first transfer until the next day with transfer
   GROUP BY
      1,
      2
)
SELECT
   *
FROM
   balance_all_days
WHERE
   balance > 0.0001 ;