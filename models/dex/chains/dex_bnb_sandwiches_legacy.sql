{{ config(
	    tags=['legacy'],
        schema = 'dex_bnb',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1