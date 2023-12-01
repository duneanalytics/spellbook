{{config(
        schema = 'cex_zora',
        alias = 'addresses'
        )}}

SELECT 'zora' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}