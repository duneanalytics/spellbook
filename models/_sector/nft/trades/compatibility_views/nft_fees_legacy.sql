{{ config(
	tags=['legacy'],
	
        schema = 'nft',
        alias = alias('fees', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum","solana","bnb", "optimism","arbitrum","polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke", "0xRob", "hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('nft_events_legacy') }}
WHERE evt_type = "Trade"
