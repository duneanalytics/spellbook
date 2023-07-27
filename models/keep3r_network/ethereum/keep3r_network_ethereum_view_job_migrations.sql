{{ config(
    tags=['legacy'],
    alias = alias('job_migrations', legacy_model=True),
    post_hook = '{{ expose_spells_hide_trino(\'["ethereum"]\', "project", "keep3r", \'["wei3erHase", "agaperste"]\') }}'
) }}

SELECT
    evt_block_time AS `timestamp`,
    evt_tx_hash AS tx_hash,
    evt_index + s.step AS evt_index,
    CASE
        s.step
        WHEN (0) THEN 'JobMigrationOut'
        WHEN (1) THEN 'JobMigrationIn'
    END AS event,
    contract_address AS keep3r,
    CASE
        s.step
        WHEN (0) THEN m._fromJob
        WHEN (1) THEN m._toJob
    END AS job
FROM
    (
        SELECT
            evt_block_time,
            evt_tx_hash,
            evt_index,
            contract_address,
            _fromJob,
            _toJob
        FROM
            {{ source(
                'keep3r_network_ethereum',
                'Keep3r_evt_JobMigrationSuccessful'
            ) }}
        UNION
        SELECT
            evt_block_time,
            evt_tx_hash,
            evt_index,
            contract_address,
            _fromJob,
            _toJob
        FROM
            {{ source(
                'keep3r_network_ethereum',
                'Keep3r_v2_evt_JobMigrationSuccessful'
            ) }}
    ) AS m
    INNER JOIN (
        SELECT
            explode(SEQUENCE(0, 1)) AS step
    ) AS s
    ON TRUE
