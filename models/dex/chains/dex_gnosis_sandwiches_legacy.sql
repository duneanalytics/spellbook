{{ config(
	    tags=['legacy'],
        schema = 'dex_gnosis',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1