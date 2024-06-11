{{ config(
    alias = 'product_information',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["guyhowlett"]\') }}'
    )
}}

SELECT product_contract_address,
       product_name,
       product_type,
       date_added,
       'v1' AS syndicate
FROM {{ ref('nexusmutual_ethereum_products_v1') }}
