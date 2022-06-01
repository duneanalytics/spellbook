{{ config(
        alias='erc20_noncompliant',
        materialized ='table'
) 
}}

select distinct token_address
from {{ ref('transfers_ethereum_erc20_rolling_day') }}
where round(amount/power(10, 18), 6) < -0.001