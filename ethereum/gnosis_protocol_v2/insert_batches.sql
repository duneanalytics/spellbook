CREATE OR REPLACE FUNCTION gnosis_protocol_v2.insert_batches(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
    LANGUAGE plpgsql AS
$function$
DECLARE
    r integer;
BEGIN;
    WITH rows AS (
        WITH batch_counts AS (
            SELECT s.evt_block_time,
                   s.evt_tx_hash,
                   solver,
                   (select count(*)
                    from gnosis_protocol_v2."GPv2Settlement_evt_Trade" t
                    where t.evt_tx_hash = s.evt_tx_hash)                                                  as num_trades,
                   sum(case when selector != '\x2e1a7d4d' and selector != '\x095ea7b3' then 1 else 0 end) as dex_swaps,
                   sum(case when selector = '\x2e1a7d4d' then 1 else 0 end)                               as unwraps,
                   sum(case when selector = '\x095ea7b3' then 1 else 0 end)                               as token_approvals
            FROM gnosis_protocol_v2."GPv2Settlement_evt_Settlement" s
                     LEFT OUTER JOIN gnosis_protocol_v2."GPv2Settlement_evt_Interaction" i
                                     ON i.evt_tx_hash = s.evt_tx_hash
            WHERE s.evt_block_time >= start_ts
              AND s.evt_block_time < end_ts
            GROUP BY s.evt_tx_hash, solver, s.evt_block_time
            ),


            batch_values as (
                select tx_hash,
                       sum(trade_value_usd) as batch_value,
                       sum(fee_usd)         as fee_value,
                       price                as eth_price
                from gnosis_protocol_v2.trades
                         JOIN prices.usd as p
                              ON p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                  AND p.minute = date_trunc('minute', block_time)
                where block_time >= start_ts
                  and block_time < end_ts
                group by tx_hash, price
                ),

            batch_info as (
                SELECT evt_block_time                               as block_time,
                       num_trades,
                       CASE
                           WHEN name = '1inch'
                               OR name = 'ParaSwap'
                               OR name = '0x'
                               OR name = 'Legacy'
                               THEN (select count(*) from dex.trades where tx_hash = evt_tx_hash and category = 'DEX')
                           ELSE dex_swaps END                       as dex_swaps,
                       batch_value,
                       tx.gas_used / num_trades                     as gas_per_trade,
                       solver                                       as solver_address,
                       CONCAT(environment, CONCAT('-', name))       as solver_name,
                       evt_tx_hash                                  as tx_hash,
                       gas_price / pow(10, 9)                       as gas_price_gwei,
                       tx.gas_used                                  as gas_used,
                       (gas_price * gas_used * eth_price) / 10 ^ 18 as tx_cost_usd,
                       fee_value,
                       length(data)::decimal / 1024                 as call_data_size,
                       unwraps,
                       token_approvals
                FROM batch_counts b
                         LEFT OUTER JOIN batch_values t
                                         ON b.evt_tx_hash = t.tx_hash
                         JOIN ethereum.transactions tx
                              ON evt_tx_hash = hash
                         JOIN gnosis_protocol_v2.view_solvers
                              ON solver = address
                  WHERE num_trades > 0 -- Exclude Withdrawal Batches
                )
            INSERT INTO gnosis_protocol_v2.batches
                (block_time, num_trades, dex_swaps, batch_value, gas_per_trade, solver_address, solver_name, tx_hash,
                 gas_price_gwei, gas_used, tx_cost_usd, fee_value, call_data_size, unwraps, token_approvals)
                SELECT block_time,
                       num_trades,
                       dex_swaps,
                       batch_value,
                       gas_per_trade,
                       solver_address,
                       solver_name,
                       tx_hash,
                       gas_price_gwei,
                       gas_used,
                       tx_cost_usd,
                       fee_value,
                       call_data_size,
                       unwraps,
                       token_approvals
                FROM batch_info
                ORDER BY block_time DESC
                -- conflict is by dropping potentially conflicting before executing the insertion (cf. the cronjobs below)
                ON CONFLICT DO NOTHING
                RETURNING 1)

    SELECT count(*)
    INTO r
    FROM rows;
    RETURN r;
END
$function$;

COMMIT;

-- fill 2021: This is only ever relevant 1 time.
SELECT gnosis_protocol_v2.insert_batches(
               '2021-03-03', --! Deployment date
               '2022-01-01'
           )
WHERE NOT EXISTS(
        SELECT *
        FROM gnosis_protocol_v2.batches
        WHERE block_time >= '2021-03-03'
          AND block_time < '2022-01-01'
    );

-- For the two cron jobs defined below,
-- one is intended to back-fill lagging price feed (while also including most recent trades).
-- The second, less frequent job is meant to back-fill missing token data that is manually
-- updated on an irregular schedule. Although there is no token data directly here,
-- this table depends on gnosis_protocol_v2.trades which does rely on token data.
-- A six month time window should suffice for manual token updates.

-- Every five minutes we go back 1 day and repopulate the values.
-- This captures new batches since the previous run, but also includes
-- previously non-existent price data (since the price feed is slightly behind)
INSERT INTO cron.job (schedule, command)
VALUES ('*/5 * * * *', $$
    BEGIN;
    DELETE FROM gnosis_protocol_v2.batches
        WHERE block_time >= (SELECT DATE_TRUNC('day', now()) - INTERVAL '1 days');
    SELECT gnosis_protocol_v2.insert_batches(
        (SELECT DATE_TRUNC('day', now()) - INTERVAL '1 days')
    );
    COMMIT;
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

-- Once per day we go back 6 months repopulate the values.
-- This is intended to back fill any new token data and prices that may have been introduced.
-- While simultaneously updating all fields relying on token data. Specifically, these are:
-- batch_value and fee_value
--
-- NOTE that we choose to run this job daily at 11 minutes past midnight,
-- so not to compete with the every 5 minute job above and the updating trades table which this depends on.
INSERT INTO cron.job (schedule, command)
VALUES ('11 0 * * *', $$
    BEGIN;
    DELETE FROM gnosis_protocol_v2.batches
        WHERE block_time >= (SELECT DATE_TRUNC('day', now()) - INTERVAL '6 months');
    SELECT gnosis_protocol_v2.insert_batches(
        (SELECT DATE_TRUNC('day', now()) - INTERVAL '6 months')
    );
    COMMIT;
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
