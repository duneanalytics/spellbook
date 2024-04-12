{{config(
        schema = 'cex_zksync',
        alias = 'addresses'
        )}}

SELECT 'zksync' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}