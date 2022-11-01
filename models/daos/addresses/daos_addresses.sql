{{ config(
    alias = 'addresses',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", gnosis"]\',
                                "sector",
                                "daos",
                                \'["henrystats"]\') }}')
}}


SELECT * FROM {{ ref('daos_addresses_ethereum') }}

UNION ALL 

SELECT * FROM {{ ref('daos_addresses_gnosis') }}