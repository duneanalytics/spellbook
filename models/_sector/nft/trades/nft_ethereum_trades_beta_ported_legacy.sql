{{ config(
	tags=['legacy'],
	
    schema = 'nft_ethereum',
    alias = alias('trades_beta_ported', legacy_model=True),
    materialized = 'view'
    )
}}

{{ port_to_old_schema_legacy(ref('nft_ethereum_trades_beta_legacy')) }}
