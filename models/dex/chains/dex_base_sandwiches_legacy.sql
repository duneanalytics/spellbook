{{ config(
	    tags=['legacy'],
        schema = 'dex_base',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1