{{ config(
    alias = 'addresses',
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["henrystats"]\') }}')
}}


SELECT * FROM {{ ref('dao_addresses_ethereum') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_gnosis') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_polygon') }}