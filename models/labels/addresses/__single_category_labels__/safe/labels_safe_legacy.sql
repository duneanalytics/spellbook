{{config(
	tags=['legacy'],
	alias = alias('safe', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT * FROM {{ ref('labels_safe_ethereum_legacy') }}