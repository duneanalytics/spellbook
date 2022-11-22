BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_balances;
CREATE MATERIALIZED VIEW gnosis_protocol.view_balances AS
WITH
last_movement AS (
    SELECT
        MAX(batch_id) AS batch_id,
        MAX(movement_date) AS movement_date,
        trader,
        token
    FROM gnosis_protocol.view_movement
    GROUP BY trader, token
)
SELECT
    movement.trader,
    movement.token_symbol,
    movement.token,
    movement.decimals,
    movement.balance,
    movement.balance_deposited,
    movement.balance_deposited_atoms,
    movement.balance_actual,
    movement.balance_actual_atoms,
    last_movement.movement_date AS last_movement_date,
    last_movement.batch_id AS last_movement_batch_id
FROM last_movement
JOIN gnosis_protocol.view_movement movement
    ON movement.batch_id = last_movement.batch_id
    AND movement.trader = last_movement.trader
    AND movement.token = last_movement.token;


CREATE UNIQUE INDEX IF NOT EXISTS view_balances_id ON gnosis_protocol.view_balances (trader, token) ;
CREATE INDEX view_balances_1 ON gnosis_protocol.view_balances (token);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_balances')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
