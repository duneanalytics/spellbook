{{config(
	tags=['legacy'],
	alias = alias('funds', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT * FROM {{ ref('labels_funds_ethereum_legacy') }}
