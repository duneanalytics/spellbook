{{ 
    config(
        schema = 'eigenlayer',
        alias = 'rewards_v1_flattened',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


SELECT
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    avs,
    CAST(regexp_extract(rewardsSubmission, '"token":"([^"]+)"', 1) AS VARCHAR) AS token,
    CAST(regexp_extract(rewardsSubmission, '"amount":([0-9]+)', 1) AS DECIMAL(38, 0)) AS amount
FROM
    {{ source('eigenlayer_ethereum', 'RewardsCoordinator_evt_AVSRewardsSubmissionCreated') }}
