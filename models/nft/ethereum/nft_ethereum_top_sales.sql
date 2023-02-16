{{ config(
     alias = 'top_sales',
     materialized='incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['rn'],
     post_hook='{{ expose_spells(\'["ethereum"]\',
                                 "sector",
                                 "nft",
                                 \'["Henrystats"]\') }}'
     )
 }}

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
WHERE 
    blockchain = 'ethereum'
    AND currency_symbol IN ('ETH', 'WETH')
    AND amount_original IS NOT NULL 
    AND number_of_items = 1
QUALIFY 
    rn <= 50
{% else %}
with incremental_subset as
(
    SELECT
        nft_contract_address, 
        token_id as nft_token_id,
        tx_hash
    FROM 
        {{ ref('nft_trades') }}
    WHERE
        block_time >= date_trunc("day", now() - interval '1 week')
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('ETH', 'WETH')
        AND amount_original IS NOT NULL 
        AND number_of_items = 1
)
SELECT 
    src.nft_contract_address, 
    src.token_id as nft_token_id, 
    src.seller, 
    src.amount_original as price, 
    src.tx_hash,
    ROW_NUMBER() OVER (PARTITION BY src.nft_contract_address ORDER BY src.amount_original DESC) as rn
FROM 
    {{ ref('nft_trades') }} src
INNER JOIN
    incremental_subset --reduce number of rows to process on incremental run(s)
    ON src.nft_contract_address = incremental_subset.nft_contract_address
    AND src.nft_token_id = incremental_subset.nft_token_id
    AND src.tx_hash = incremental_subset.tx_hash
WHERE 
    blockchain = 'ethereum'
    AND currency_symbol IN ('ETH', 'WETH')
    AND amount_original IS NOT NULL 
    AND number_of_items = 1
QUALIFY 
    rn <= 50
{% endif %}