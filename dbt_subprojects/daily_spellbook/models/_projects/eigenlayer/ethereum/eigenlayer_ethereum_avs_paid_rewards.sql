{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'avs_paid_rewards',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['avs']
    )
}}


SELECT DISTINCT
  a.avs,
  b.metadataURI
FROM {{ ref('eigenlayer_ethereum_rewards_v1_flattened') }} AS a
LEFT JOIN {{ ref('eigenlayer_ethereum_avs_metadata_uri_latest') }} AS b
  ON a.avs = b.avs
