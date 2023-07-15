{{ config(
    tags=['legacy', "legacy"],
    alias = alias('job_liquidity_log', legacy_model=True),
    post_hook = '{{ expose_spells_hide_trino(\'["ethereum"]\', "project", "keep3r", \'["wei3erHase", "agaperste"]\') }}'
) }}

WITH job_liquidities AS (

    SELECT
        ad.evt_block_time AS `timestamp`,
        ad.evt_tx_hash AS tx_hash,
        ad.evt_index,
        'LiquidityAddition' AS event,
        ad.contract_address AS keep3r,
        ad._job AS job,
        ad._liquidity AS token,
        CAST(ad._amount AS DOUBLE) / 1e18 AS amount
    FROM
        (
            SELECT
                evt_block_time,
                evt_tx_hash,
                evt_index,
                contract_address,
                _job,
                _liquidity,
                _amount
            FROM
                {{ source(
                    'keep3r_network_ethereum',
                    'Keep3r_evt_LiquidityAddition'
                ) }}
            UNION
            SELECT
                evt_block_time,
                evt_tx_hash,
                evt_index,
                contract_address,
                _job,
                _liquidity,
                _amount
            FROM
                {{ source(
                    'keep3r_network_ethereum',
                    'Keep3r_v2_evt_LiquidityAddition'
                ) }}
        ) ad
    UNION ALL
    SELECT
        rm.evt_block_time AS `timestamp`,
        rm.evt_tx_hash AS tx_hash,
        rm.evt_index,
        'LiquidityWithdrawal' AS event,
        rm.contract_address keep3r,
        rm._job job,
        rm._liquidity AS token,- CAST(rm._amount AS DOUBLE) / 1e18 AS amount
    FROM
        (
            SELECT
                evt_block_time,
                evt_tx_hash,
                evt_index,
                contract_address,
                _job,
                _liquidity,
                _amount
            FROM
                {{ source(
                    'keep3r_network_ethereum',
                    'Keep3r_evt_LiquidityWithdrawal'
                ) }}
            UNION
            SELECT
                evt_block_time,
                evt_tx_hash,
                evt_index,
                contract_address,
                _job,
                _liquidity,
                _amount
            FROM
                {{ source(
                    'keep3r_network_ethereum',
                    'Keep3r_v2_evt_LiquidityWithdrawal'
                ) }}
        ) rm
),
df AS (
    SELECT
        `timestamp`,
        tx_hash,
        evt_index,
        event,
        keep3r,
        job,
        token,
        amount
    FROM
        job_liquidities
    UNION
    SELECT
        migs.event,
        migs.evt_index,
        migs.job,
        migs.keep3r,
        migs.`timestamp`,
        migs.tx_hash,
        liqs.token AS token,
        NULL AS amount
    FROM
        {{ ref('keep3r_network_ethereum_view_job_migrations_legacy') }} AS migs
        INNER JOIN (
            -- generates 1 extra line per token of keep3r
            SELECT
                DISTINCT keep3r,
                job,
                token
            FROM
                job_liquidities
        ) liqs
        ON migs.keep3r = liqs.keep3r
),
migration_out AS (
    SELECT
        *,
        CASE
            WHEN event = 'JobMigrationOut' THEN SUM(
                - amount
            ) over (
                PARTITION BY keep3r,
                job,
                token rows unbounded preceding
            )
        END AS migration_out
    FROM
        df
),
migration_in AS (
    SELECT
        *,
        CASE
            WHEN event = 'JobMigrationIn' THEN LAG(
                - migration_out
            ) over (
                PARTITION BY tx_hash,
                keep3r,
                token
                ORDER BY
                    evt_index
            )
        END AS migration_in
    FROM
        migration_out
)
SELECT
    `timestamp`,
    tx_hash,
    evt_index,
    event,
    keep3r,
    job,
    token,
    COALESCE(
        amount,
        migration_out,
        migration_in
    ) AS amount
FROM
    migration_in
ORDER BY
    `timestamp`
