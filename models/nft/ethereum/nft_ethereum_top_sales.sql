{{ config(
     alias = 'top_sales',
     materialized='incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['tx_hash', 'nft_contract_address', 'nft_token_id', 'price', 'rn'],
     post_hook='{{ expose_spells(\'["ethereum"]\',
                                 "sector",
                                 "nft",
                                 \'["Henrystats"]\') }}'
     )
 }}

WITH 

sales as (
 {% if not is_incremental() %}
    SELECT 
        nft_contract_address, 
        token_id as nft_token_id, 
        seller, 
        amount_original as price, 
        tx_hash, 
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY amount_original DESC) as rn 
    FROM 
    {{ ref('nft_trades') }}
    WHERE blockchain = 'ethereum'
    AND currency_symbol IN ('ETH', 'WETH')
    QUALIFY rn <= 50 
{% endif %}
{% if is_incremental() %}
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY price DESC) as rn 
    FROM 
    (
        SELECT 
            nft_contract_address, nft_token_id, seller, price, tx_hash
        FROM 
        {{this}}

        UNION 

        SELECT 
            nft_contract_address,
            token_id as nft_token_id, 
            seller, 
            amount_original as price, 
            tx_hash
        FROM 
        {{ ref('nft_trades') }}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('ETH', 'WETH')
        AND amount_original >= (SELECT MIN(price) FROM {{this}}) -- optimize query
    ) x 
    QUALIFY rn <= 50
{% endif %}
)

SELECT 
    nft_contract_address, 
    nft_token_id,
    seller, 
    price, 
    tx_hash, 
    rn 
FROM 
sales 
ORDER BY rn DESC 