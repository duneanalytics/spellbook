{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['name','address'],
    )
}}

SELECT * FROM {{ ref('static_labels_all') }}
UNION
SELECT * FROM {{ ref('query_labels_all') }}