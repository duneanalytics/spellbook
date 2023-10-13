{{ config(
    schema = 'sofi',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True),
    post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "sofi",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 1