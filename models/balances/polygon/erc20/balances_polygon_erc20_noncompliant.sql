{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_noncompliant'),
        materialized ='table',
        file_format = 'delta'
        )
}}

SELECT  
    DISTINCT token_address
FROM 
{{ ref('transfers_polygon_erc20_rolling_day') }}
WHERE round(amount/power(10, 18), 6) < -0.001