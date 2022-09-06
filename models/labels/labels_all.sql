{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta')
}}

-- Query Labels
SELECT * FROM {{ ref('labels_nft') }}