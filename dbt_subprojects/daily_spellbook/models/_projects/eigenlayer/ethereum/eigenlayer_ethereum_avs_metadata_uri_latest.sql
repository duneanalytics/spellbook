{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'avs_metadata_uri_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['avs']
    )
}}


WITH latest_status AS (
    SELECT
        avs,
        metadataURI,
        ROW_NUMBER() OVER (PARTITION BY avs ORDER BY evt_block_number DESC) AS rn
    FROM {{ source('eigenlayer_ethereum', 'AVSDirectory_evt_AVSMetadataURIUpdated') }}
)
SELECT
    avs,
    metadataURI
FROM latest_status
WHERE rn = 1;
