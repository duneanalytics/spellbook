{{config(
        schema = 'cex_arbitrum',
        alias = 'addresses'
        )}}

SELECT 'aribitrum' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}