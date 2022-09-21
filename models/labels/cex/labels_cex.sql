{{config(alias='cex')}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}
