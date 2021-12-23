BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_batches;

CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_batches AS
WITH batch_counts AS (
    SELECT i.evt_block_time,
           i.evt_tx_hash,
           solver,
           (select count(*)
            from gnosis_protocol_v2."GPv2Settlement_evt_Trade" t
            where t.evt_tx_hash = i.evt_tx_hash)                                                  as num_trades,
           sum(case when selector != '\x2e1a7d4d' and selector != '\x095ea7b3' then 1 else 0 end) as dex_swaps,
           sum(case when selector = '\x2e1a7d4d' then 1 else 0 end)                               as unwraps,
           sum(case when selector = '\x095ea7b3' then 1 else 0 end)                               as token_approvals
    FROM gnosis_protocol_v2."GPv2Settlement_evt_Interaction" i
             JOIN gnosis_protocol_v2."GPv2Settlement_evt_Settlement" s
                  ON i.evt_tx_hash = s.evt_tx_hash
    GROUP BY i.evt_tx_hash, solver, i.evt_block_time
),

     batch_values as (
         select tx_hash,
                sum(trade_value_usd) as batch_value,
                sum(fee_usd)         as fee_value,
                price                as eth_price
         from gnosis_protocol_v2."view_trades"
                  JOIN prices.usd as p
                       ON p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                           AND p.minute = date_trunc('minute', block_time)
         group by tx_hash, price
     ),

     batch_info as (
         SELECT evt_block_time                               as block_time,
                num_trades,
                CASE
                    WHEN name = '1inch'
                        OR name = 'Paraswap'
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
                  JOIN ethereum."transactions" tx
                       ON evt_tx_hash = hash
                  JOIN gnosis_protocol_v2.view_solvers
                       ON solver = address
         WHERE tx.block_time > '2021-03-03' --! Contract Launch Date
           AND num_trades > 0 -- Exclude Withdrawal Batches
     )

SELECT *
FROM batch_info
ORDER BY block_time DESC;


CREATE UNIQUE INDEX IF NOT EXISTS view_batches_id ON gnosis_protocol_v2.view_batches (tx_hash);
CREATE INDEX view_batches_idx_1 ON gnosis_protocol_v2.view_batches (block_time);
CREATE INDEX view_batches_idx_2 ON gnosis_protocol_v2.view_batches (solver_address);
CREATE INDEX view_batches_idx_3 ON gnosis_protocol_v2.view_batches (num_trades);


INSERT INTO cron.job (schedule, command)
VALUES ('*/30 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_batches')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
