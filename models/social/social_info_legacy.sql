{{ config(
        tags = ['legacy', 'static'],
        schema='social',
        alias = alias('info', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb", "avalanche_c", "arbitrum", "base"]\',
                                    "sector",
                                    "social",
                                    \'["hildobby"]\') }}')
}}

SELECT 1