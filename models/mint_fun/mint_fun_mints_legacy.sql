{{
    config(
        tags = ['legacy'],
        alias = alias('mints', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "base"]\',
                                "project",
                                "mint_fun",
                                \'["hildobby"]\') }}'
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT 1