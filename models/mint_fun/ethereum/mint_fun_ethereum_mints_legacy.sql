{{
    config(
        alias = alias('mints', legacy_model=True),
        tags = ['legacy'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "mint_fun",
                                \'["hildobby"]\') }}'
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT 1