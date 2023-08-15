{{ 
    config(
	tags=['legacy'],
        alias = alias('celo', legacy_model=True), 
        tags = ['legacy']
    )
}}
SELECT 1