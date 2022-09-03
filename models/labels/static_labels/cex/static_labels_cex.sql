{{config(alias='cex',
    materialized = 'table',
    file_format = 'delta')}}

SELECT * FROM {{ ref('static_labels_cex_ethereum') }}
