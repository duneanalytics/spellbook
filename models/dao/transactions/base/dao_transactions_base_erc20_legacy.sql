{{ config(
	tags=['legacy'],
    alias = alias('transactions_base_erc20', legacy_model=True)
    )
}}

SELECT
1 as dummy 
