{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'operator_metadata_uri_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['operator']
    )
}}


WITH latest_status AS (
    SELECT
        operator,
        metadataURI,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY evt_block_number DESC) AS rn
    FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_OperatorMetadataURIUpdated') }}
)
SELECT
    operator,
    metadataURI
FROM latest_status
WHERE rn = 1;
