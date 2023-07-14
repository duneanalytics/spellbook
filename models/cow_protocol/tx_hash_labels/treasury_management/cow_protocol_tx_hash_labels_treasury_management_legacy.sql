{{
    config(
	tags=['legacy'],
	
        alias = alias('tx_hash_labels_treasury_management', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management_ethereum_legacy') }}