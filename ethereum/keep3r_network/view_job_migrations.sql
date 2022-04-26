CREATE OR REPLACE VIEW keep3r_network.view_job_migrations AS (
    SELECT
        evt_block_time AS timestamp,
        '0x' || encode(evt_tx_hash, 'hex') AS tx_hash,
        evt_index + s.step AS evt_index,
        CASE s.step
        WHEN (0) THEN
            'JobMigrationOut'
        WHEN (1) THEN
            'JobMigrationIn'
        END AS event,
        '0x' || encode(contract_address, 'hex') keep3r,
        '0x' || encode(
            CASE s.step
            WHEN (0) THEN
                m. "_fromJob"
            WHEN (1) THEN
                m. "_toJob"
            END, 'hex') AS job
    FROM (
        SELECT
            *
        FROM
            keep3r_network. "Keep3r_evt_JobMigrationSuccessful"
        UNION
        SELECT
            *
        FROM
            keep3r_network. "Keep3r_v2_evt_JobMigrationSuccessful") AS m
        INNER JOIN (
            SELECT
                generate_series(0, 1) AS step) AS s ON TRUE);
