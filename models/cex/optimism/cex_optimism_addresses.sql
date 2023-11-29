{{config(
        schema = 'cex_optimism',
        alias = 'addresses'
        )}}

SELECT 'optimism' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}