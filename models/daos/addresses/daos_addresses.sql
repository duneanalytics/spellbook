{{ config(
    alias = 'addresses',
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                "sector",
                                "daos",
                                \'["Henrystats"]\') }}')
}}


SELECT * FROM {{ ref('daos_addresses_ethereum') }}

UNION ALL 

SELECT * FROM {{ ref('daos_addresses_gnosis') }}