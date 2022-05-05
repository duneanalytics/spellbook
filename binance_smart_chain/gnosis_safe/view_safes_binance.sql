BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes_binance;

CREATE MATERIALIZED VIEW gnosis_safe.view_safes_binance AS
    SELECT
    	et.from AS address,
    	et.block_time AS creation_time
    FROM bsc.traces et 
    WHERE et.success = True
        AND et.call_type = 'delegatecall' -- The delegate call to the master copy is the Safe address
        AND substring(et."input" for 4) = '\xb63e800d' -- setup method
        AND et."to" = '\x2bb001433cf04c1f7d71e3c40fed66b2b563065e'  -- Binance custom mastercopy
        AND gas_used > 0;  -- to ensure the setup call was successful

COMMIT;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS view_safes_binance_unique_idx ON gnosis_safe.view_safes_binance (address);

INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_safe.view_safes_binance')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;