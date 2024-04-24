{{config(
        schema = 'cex_bnb',
        alias = 'addresses'
        )}}

SELECT 'bnb' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}