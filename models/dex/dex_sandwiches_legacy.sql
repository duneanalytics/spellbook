{{ config(
        tags=['legacy'],
        alias = alias('sandwiches', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT 1