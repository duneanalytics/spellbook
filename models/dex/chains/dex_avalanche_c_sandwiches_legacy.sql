{{ config(
	    tags=['legacy'],
        schema = 'dex_avalanche_c',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1