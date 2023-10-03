{{
    config(
	tags=['legacy'],

        alias = alias('tx_hash_labels_bluechip_investment', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment_ethereum_legacy') }}
