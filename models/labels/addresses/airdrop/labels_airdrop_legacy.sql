{{config(
	tags=['legacy'],
	alias = alias('airdrop', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}'
)}}

SELECT * FROM {{ ref('labels_airdrop_1_receivers_optimism_legacy') }}