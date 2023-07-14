{{ config(
	tags=['legacy'],
	
    alias = alias('addresses', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}


SELECT * FROM {{ ref('dao_addresses_ethereum_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_gnosis_legacy') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_polygon_legacy') }}