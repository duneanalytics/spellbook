{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'avs_operator_registration_status_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['avs', 'operator']
    )
}}


WITH latest_status AS (
    SELECT
        operator,
        avs,
        status,
        ROW_NUMBER() OVER (PARTITION BY operator, avs ORDER BY evt_block_number DESC) AS rn
    FROM {{ source('eigenlayer_ethereum', 'AVSDirectory_evt_OperatorAVSRegistrationStatusUpdated') }}
)
SELECT
    operator,
    avs,
    status
FROM latest_status
WHERE rn = 1 and status = 1;
