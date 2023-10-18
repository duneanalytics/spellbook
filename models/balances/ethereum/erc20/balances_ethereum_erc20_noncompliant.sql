{{ config(
        alias = alias('erc20_noncompliant'),
        materialized ='table',
        file_format = 'delta'
) 
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

select distinct token_address
from {{ ref('transfers_ethereum_erc20_rolling_day') }}
where round(amount/power(10, 18), 6) < -0.001