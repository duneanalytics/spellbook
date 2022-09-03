{{config(alias='tornado_cash_recipients',
    materialized = 'table',
    file_format = 'delta')}}

SELECT
    array(blockchain) as blockchain,
    recipient as address,
    'Tornado Cash Recipient' AS name,
    'tornado_cash' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-01') as created_at,
    now() as updated_at
FROM {{ ref('tornado_cash_withdrawals') }}