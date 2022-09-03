{{config(alias='tornado_cash')}}

SELECT * FROM {{ ref('labels_tornado_cash_depositors') }}
UNION
SELECT * FROM {{ ref('labels_tornado_cash_recipients') }}