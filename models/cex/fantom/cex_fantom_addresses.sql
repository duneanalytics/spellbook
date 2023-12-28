{{config(
        schema = 'cex_fantom',
        alias = 'addresses'
        )}}

SELECT 'fantom' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}