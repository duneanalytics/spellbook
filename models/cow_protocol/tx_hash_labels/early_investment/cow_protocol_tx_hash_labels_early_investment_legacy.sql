{{
    config(
	tags=['legacy'],
	
        alias = alias('tx_hash_labels_early_investment', legacy_model=True),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment_ethereum_legacy') }}