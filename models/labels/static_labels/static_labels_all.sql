{{config(alias='all')}}

SELECT * FROM {{ ref('static_labels_cex') }}
