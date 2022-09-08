{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta')
}}

-- Static Labels
SELECT * FROM {{ ref('labels_cex') }}