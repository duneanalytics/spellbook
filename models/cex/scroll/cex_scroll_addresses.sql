{{config(
        schema = 'cex_scroll',
        alias = 'addresses'
        )}}

SELECT 'scroll' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses')}}