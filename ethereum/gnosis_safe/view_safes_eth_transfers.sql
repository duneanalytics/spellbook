BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_safe.view_safes_eth_transfers;

CREATE MATERIALIZED VIEW gnosis_safe.view_safes_eth_transfers AS
    SELECT 
        et.tx_hash, 
        et.block_time, 
        et."from" AS address, 
        -value AS amount
    FROM ethereum.traces et
    join gnosis_safe.view_safes s
        ON et."from" = s.address
    WHERE success = TRUE
        AND value <> 0
        AND block_time > '2018-11-25'  -- There have been no Safe contracts before
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    
    UNION ALL
    
    SELECT 
        et.tx_hash, 
        et.block_time, 
        et."to" AS address, 
        value AS amount
    FROM ethereum.traces et
    JOIN gnosis_safe.view_safes s
        ON et."to" = s.address
    WHERE success = TRUE
        AND value <> 0
        AND block_time > '2018-11-25'  -- There have been no Safe contracts before
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
)

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS view_safes_eth_transfers_unique_idx ON gnosis_safe.view_safes_eth_transfers (tx_hash);
CREATE INDEX IF NOT EXISTS view_safes_eth_transfers_block_time_idx ON gnosis_safe.view_safes_eth_transfers USING BRIN (block_time);

INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_safe.view_safes_eth_transfers$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
