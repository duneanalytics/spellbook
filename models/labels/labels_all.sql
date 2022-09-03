{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta'
    )
}}

SELECT * FROM 
(
SELECT * FROM {{ ref('static_labels_all') }}
UNION
SELECT * FROM {{ ref('query_labels_all') }}
)