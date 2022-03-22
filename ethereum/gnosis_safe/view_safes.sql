BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes;

CREATE MATERIALIZED VIEW gnosis_safe.view_safes AS
    SELECT
    	et.from AS address,
    	et.block_time AS creation_time
    FROM ethereum.traces et 
    WHERE et.success = True
        AND substring(et."input" for 4) IN ('\x0ec78d9e', '\xa97ab18a', '\xb63e800d') -- setup methods of v0.1.0, v1.0.0, v1.1.0 (=v1.1.1=1.2.0)
        AND et.call_type = 'delegatecall' -- the delegate call to the master copy is the Safe address
        AND et."to" IN (
            '\x8942595A2dC5181Df0465AF0D7be08c8f23C93af', -- mastercopy address v0.1.0
            '\xb6029ea3b2c51d09a50b53ca8012feeb05bda35a', -- v1.0.0
            '\xae32496491b53841efb51829d6f886387708f99b', -- v1.1.0
            '\x34cfac646f301356faa8b21e94227e3583fe3f5f', -- v1.1.1
            '\x6851d6fdfafd08c0295c392436245e5bc78b0185'   -- v1.2.0
        )
        
    UNION ALL
    
    SELECT contract_address AS address, evt_block_time AS creation_time
    FROM gnosis_safe."GnosisSafev1.3.0_evt_SafeSetup";

INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_safe.view_safes$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
