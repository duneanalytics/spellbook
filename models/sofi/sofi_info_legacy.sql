{{ config(
        tags = ['legacy', 'static'],
        schema='sofi',
        alias = alias('info', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb", "avalanche_c", "arbitrum", "base"]\',
                                    "sector",
                                    "sofi",
                                    \'["hildobby"]\') }}')
}}

SELECT 1