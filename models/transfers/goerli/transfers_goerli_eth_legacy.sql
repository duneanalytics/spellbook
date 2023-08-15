{{ 
    config(
	tags=['legacy'],
        alias = alias('goerli', legacy_model=True), 
        tags = ['legacy']
    )
}}
SELECT 1