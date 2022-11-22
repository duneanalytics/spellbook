{{config(
    alias='wallet_funders_wallets', 
    unique_key = ['wallet']
    )   
}}


SELECT DISTINCT(buyer) as wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION 

SELECT DISTINCT(seller) as wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION 

SELECT DISTINCT(buyer) as wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION

SELECT DISTINCT(seller) as wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION 

SELECT DISTINCT(buyer) as wallet FROM {{ ref('blur_ethereum_trades') }}

UNION

SELECT DISTINCT(seller) as wallet FROM {{ ref('blur_ethereum_trades') }}