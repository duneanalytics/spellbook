{{config(
        schema = 'cex_gnosis',
        alias = 'addresses'
        )}}

SELECT 'gnosis' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}