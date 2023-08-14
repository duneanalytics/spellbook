{{ config(
        schema='prices',
        alias = alias('usd_latest_old'),
        tags= ['static', 'dunesql'],
        materialized='table',
        file_format = 'delta'
        )
}}

SELECT
  pu.blockchain
, pu.contract_address
, pu.decimals
, pu.symbol
, max(pu.minute) as minute
, max_by(pu.price, pu.minute) as price
FROM {{ source('prices', 'usd') }} pu
GROUP BY 1,2,3,4
