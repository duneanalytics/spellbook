{{ config(
    alias = alias('transactions'),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}




SELECT * FROM {{ ref('dao_transactions_ethereum_erc20') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_ethereum_eth') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_gnosis_erc20') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_gnosis_eth') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_polygon_erc20') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_polygon_eth') }}