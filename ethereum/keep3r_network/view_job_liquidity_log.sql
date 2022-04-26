CREATE OR REPLACE VIEW keep3r_network.view_job_liquidity_log AS (
    WITH job_liquidities AS (
        SELECT
            ad. "evt_block_time" AS TIMESTAMP,
            '0x' || encode(ad. "evt_tx_hash", 'hex') AS tx_hash,
            evt_index,
            'LiquidityAddition' AS event,
            '0x' || encode(ad. "contract_address", 'hex') keep3r,
            '0x' || encode(ad. "_job", 'hex') job,
            '0x' || encode(ad. "_liquidity", 'hex') AS token,
            ad. "_amount" / 1e18 AS amount
        FROM (
            SELECT
                *
            FROM
                keep3r_network. "Keep3r_evt_LiquidityAddition"
            UNION
            SELECT
                *
            FROM
                keep3r_network. "Keep3r_v2_evt_LiquidityAddition")
            ad
        UNION ALL
        SELECT
            rm. "evt_block_time" AS TIMESTAMP,
            '0x' || encode(rm. "evt_tx_hash", 'hex') AS tx_hash,
            evt_index,
            'LiquidityWithdrawal' AS event,
            '0x' || encode(rm. "contract_address", 'hex') keep3r,
            '0x' || encode(rm. "_job", 'hex') job,
            '0x' || encode(rm. "_liquidity", 'hex') AS token,
            - rm. "_amount" / 1e18 AS amount
        FROM (
            SELECT
                *
            FROM
                keep3r_network. "Keep3r_evt_LiquidityWithdrawal"
            UNION
            SELECT
                *
            FROM
                keep3r_network. "Keep3r_v2_evt_LiquidityWithdrawal") rm),
        df AS (
            SELECT
                *
            FROM
                job_liquidities
            UNION
            SELECT
                migs.*,
                liqs.token AS token,
                NULL AS amount
            FROM
                keep3r_network.view_job_migrations migs
                INNER JOIN (
                    -- generates 1 extra line per token of keep3r
                    SELECT DISTINCT
                        keep3r,
                        job,
                        token
                    FROM
                        job_liquidities) liqs ON migs.keep3r = liqs.keep3r),
                migration_out AS (
                    SELECT
                        *,
                        CASE WHEN event = 'JobMigrationOut' THEN
                            sum(- amount) OVER (PARTITION BY keep3r,
                                job,
                                token ROWS UNBOUNDED PRECEDING)
                        END AS migration_out
                    FROM
                        df),
                    migration_in AS (
                        SELECT
                            *,
                            CASE WHEN event = 'JobMigrationIn' THEN
                                lag(- migration_out) OVER (PARTITION BY tx_hash,
                                    keep3r,
                                    token ORDER BY evt_index)
                            END AS migration_in
                        FROM
                            migration_out
)
                        SELECT
                            timestamp,
                            tx_hash,
                            evt_index,
                            event,
                            keep3r,
                            job,
                            token,
                            COALESCE(amount, migration_out, migration_in) AS amount
                        FROM
                            migration_in
                        ORDER BY
                            timestamp)
