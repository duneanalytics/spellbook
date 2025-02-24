{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'programmatic_incentive_flattened',
    )
}}


SELECT
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    CAST(regexp_extract(rewardsSubmission, '"token":"([^"]+)"', 1) AS VARCHAR) AS token,
    CAST(regexp_extract(rewardsSubmission, '"amount":([0-9]+)', 1) AS DECIMAL(38, 0)) AS amount
FROM
    {{ source('eigenlayer_ethereum', 'RewardsCoordinator_evt_RewardsSubmissionForAllEarnersCreated') }}
