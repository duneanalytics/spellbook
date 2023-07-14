{{ config(
	tags=['legacy'],
	
    alias = alias('transactions', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}




SELECT * FROM {{ ref('dao_transactions_ethereum_erc20_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_ethereum_eth_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_gnosis_erc20_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_gnosis_eth_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_polygon_erc20_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_transactions_polygon_eth_legacy') }}