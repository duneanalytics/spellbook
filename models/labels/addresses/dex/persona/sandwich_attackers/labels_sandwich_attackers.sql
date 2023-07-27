{{
    config(
	tags=['legacy'],
	
        alias = alias('sandwich_attackers', legacy_model=True), 
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["alexth"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_sandwich_attackers_ethereum_legacy') }}