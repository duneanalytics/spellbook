{{config(
	tags=['legacy'],
	alias = alias('validators', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","bnb","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT * FROM  {{ ref('labels_validators_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_validators_bnb') }}
UNION
SELECT * FROM  {{ ref('labels_validators_solana') }}