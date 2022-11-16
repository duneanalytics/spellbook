{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tx_hash_labels",
                                \'["gentrexha"]\') }}')
}}

-- Query Labels
SELECT * FROM {{ ref('stable_to_stable') }}