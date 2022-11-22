BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes_circles;

CREATE MATERIALIZED VIEW gnosis_safe.view_safes_circles AS
    SELECT
    	et.from AS address,
    	et.block_time AS creation_time
    FROM xdai.traces et 
    WHERE et.success = True
        AND et.call_type = 'delegatecall' -- The delegate call to the master copy is the Safe address
        AND substring(et."input" for 4) = '\xb63e800d' -- setup methods of Circles mastercopy
        AND et."to" ='\x2CB0ebc503dE87CFD8f0eCEED8197bF7850184ae'  -- Circles mastercopy
        AND gas_used > 0;  -- to ensure the setup call was successful

COMMIT;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS view_safes_circles_unique_idx ON gnosis_safe.view_safes_circles (address);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('0 0 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_safe.view_safes_circles')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;