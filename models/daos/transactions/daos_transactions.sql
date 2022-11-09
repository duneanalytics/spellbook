{{ config(
    alias = 'transactions',
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                "sector",
                                "daos",
                                \'["Henrystats"]\') }}')
}}




SELECT * FROM {{ ref('daos_transactions_ethereum_erc20') }}

UNION ALL 

SELECT * FROM {{ ref('daos_transactions_ethereum_eth') }}

UNION ALL 

SELECT * FROM {{ ref('daos_transactions_gnosis_erc20') }}

UNION ALL 

SELECT * FROM {{ ref('daos_transactions_gnosis_eth') }}

