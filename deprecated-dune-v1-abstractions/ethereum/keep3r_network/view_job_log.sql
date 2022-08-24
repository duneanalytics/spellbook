CREATE OR REPLACE VIEW keep3r_network.view_job_log AS (
    SELECT
        timestamp,
        tx_hash,
        evt_index,
        event,
        keep3r,
        job,
        NULL AS keeper,
        token,
        amount,
        NULL AS period_credits
    FROM
        keep3r_network.view_job_liquidity_log
    UNION ALL
    SELECT
        *
    FROM
        keep3r_network.view_job_credits_log
    ORDER BY
        timestamp,
        evt_index);
