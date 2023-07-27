{{ config(
	tags=['legacy'],
	
        schema = 'nft',
        alias = alias('burns', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum","solana","bnb"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","0xRob"]\') }}')
}}


SELECT *
FROM {{ ref('nft_events_old') }}
WHERE evt_type = "Burn"
