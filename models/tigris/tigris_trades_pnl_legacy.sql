{{ config(
	tags=['legacy'],
	
        alias = alias('trades_pnl', legacy_model=True)
        )
}}

SELECT 
    1 