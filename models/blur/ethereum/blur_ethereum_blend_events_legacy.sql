{{
    config(
        tags=['legacy'],
        schema = 'blur_ethereum',
        alias = alias('blend_events', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "blur",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 1