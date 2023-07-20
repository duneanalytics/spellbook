{{
    config(
	tags=['legacy'],
	
        alias = alias('tx_hash_labels_offramp', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp_ethereum_legacy') }}