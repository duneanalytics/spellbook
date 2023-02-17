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
WITH src AS
(
    SELECT
        nft_contract_address
        , token_id as nft_token_id
        , seller
        , amount_original as price
        , tx_hash
        , ROW_NUMBER() OVER (ORDER BY amount_original DESC) as rn
    FROM
        {{ ref('nft_trades') }}
    WHERE 
        blockchain = 'ethereum'
        AND currency_symbol IN ('ETH', 'WETH')
        AND amount_original IS NOT NULL
        AND number_of_items = 1
)
SELECT
    *
FROM
    src
WHERE
    rn <= 50
{% else %}
WITH incremental_subset AS
(
    SELECT DISTINCT
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
, src AS
(
    SELECT
        src.nft_contract_address
        , src.token_id as nft_token_id
        , src.seller
        , src.amount_original as price
        , src.tx_hash
        , ROW_NUMBER() OVER (ORDER BY src.amount_original DESC) as rn
    FROM 
        {{ ref('nft_trades') }} src
    INNER JOIN
        incremental_subset --reduce number of rows to process on incremental run(s)
        ON src.nft_contract_address = incremental_subset.nft_contract_address
        AND src.token_id = incremental_subset.nft_token_id
        AND src.tx_hash = incremental_subset.tx_hash
    WHERE 
        src.blockchain = 'ethereum'
        AND src.currency_symbol IN ('ETH', 'WETH')
        AND src.amount_original IS NOT NULL 
        AND src.number_of_items = 1
)
SELECT
    *
FROM
    src
WHERE
    rn <= 50
{% endif %}