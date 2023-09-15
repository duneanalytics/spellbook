{{ config(
	    tags=['legacy'],
        schema = 'dex_ethereum',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1