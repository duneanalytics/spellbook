{{config(alias='funds',
    materialized = 'table',
    file_format = 'delta')}}

SELECT * FROM {{ ref('static_labels_funds_ethereum') }}
