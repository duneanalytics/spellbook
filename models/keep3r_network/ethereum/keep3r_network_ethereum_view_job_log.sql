{{ config (
    alias = 'job_log',
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "keep3r", \'["wei3erHase", "agaperste"]\') }}'
) }}

SELECT
    `timestamp`,
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
    {{ ref('keep3r_network_ethereum_view_job_liquidity_log') }}
UNION ALL
SELECT
    amount,
    event,
    evt_index,
    job,
    keep3r,
    keeper,
    period_credits,
    `timestamp`,
    token,
    tx_hash
FROM
    {{ ref('keep3r_network_ethereum_view_job_credits_log') }}
ORDER BY
    `timestamp`,
    evt_index
