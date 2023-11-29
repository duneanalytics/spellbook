{{config(
        schema = 'cex_celo',
        alias = 'addresses'
        )}}

SELECT 'celo' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}