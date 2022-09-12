{{config(alias='funds')}}

SELECT * FROM {{ ref('labels_funds_ethereum') }}
