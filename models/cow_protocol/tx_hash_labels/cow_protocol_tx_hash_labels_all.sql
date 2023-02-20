{{ config(
    alias = 'tx_hash_labels_all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tx_hash_labels",
                                \'["gentrexha"]\') }}')
}}

-- Query Labels
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable') }}