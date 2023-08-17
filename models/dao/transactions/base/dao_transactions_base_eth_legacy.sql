{{ config(
	tags=['legacy'],
    alias = alias('transactions_base_eth', legacy_model=True)
    )
}}

SELECT 
    1 as dummy

