{{ config(
	    tags=['legacy'],
        schema = 'dex_arbitrum',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1