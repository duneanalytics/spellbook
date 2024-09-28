{{ config(
    schema = 'nexusmutual_ethereum',
    alias = 'product_information',
    materialized = 'view'
    )
}}

SELECT product_contract_address,
       product_name,
       product_type,
       date_added,
       'v1' AS syndicate
FROM {{ ref('nexusmutual_ethereum_products_v1') }}
