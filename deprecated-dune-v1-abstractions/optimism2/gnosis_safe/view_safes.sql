BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes;

CREATE MATERIALIZED VIEW gnosis_safe.view_safes AS
    SELECT contract_address AS address, evt_block_time AS creation_time
    FROM gnosis_safe."GnosisSafeL2_v1_3_0_evt_SafeSetup";

COMMIT;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS view_safes_unique_idx ON gnosis_safe.view_safes (address);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('0 0 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_safe.view_safes')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;