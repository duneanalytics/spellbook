{{config(
        schema = 'cex_base',
        alias = 'addresses'
        )}}

SELECT 'base' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}