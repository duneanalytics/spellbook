{{config(
        schema = 'cex_polygon',
        alias = 'addresses'
        )}}

SELECT 'polygon' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}