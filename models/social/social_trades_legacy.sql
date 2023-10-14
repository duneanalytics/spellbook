{{ config(
    schema = 'social',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True),
    post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "social",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 1