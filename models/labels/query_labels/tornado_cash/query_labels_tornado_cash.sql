{{config(alias='tornado_cash',
    materialized = 'table',
    file_format = 'delta')}}

SELECT * FROM {{ ref('query_labels_tornado_cash_depositors') }}
UNION
SELECT * FROM {{ ref('query_labels_tornado_cash_recipients') }}