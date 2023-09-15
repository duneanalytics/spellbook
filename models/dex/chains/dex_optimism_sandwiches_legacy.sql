{{ config(
	    tags=['legacy'],
        schema = 'dex_optimism',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1