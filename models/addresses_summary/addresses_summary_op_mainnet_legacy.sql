{{ config(
	tags=['legacy'],

        alias = alias('op_mainnet', legacy_model=True)
        )
}}

SELECT
    1 as dummmy
