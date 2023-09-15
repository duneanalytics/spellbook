{{ config(
	    tags=['legacy'],
        schema = 'dex_polygon',
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1