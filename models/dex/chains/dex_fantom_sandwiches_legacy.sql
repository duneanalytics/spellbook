{{ config(
	    tags=['legacy'],
        schema = 'dex_fantom',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["fantom"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1