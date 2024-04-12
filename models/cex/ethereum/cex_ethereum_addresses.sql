{{config(
        schema = 'cex_ethereum',
        alias = 'addresses'
        )}}

SELECT 'ethereum' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}