{{
    config(
        alias = 'likely_bot_addresses',

        post_hook='{{ expose_spells('["zora"]',
        "sector",
        "labels",
        '["msilb7"]') }}'
    )
}}

{{ get_likely_bot_addresses('zora') }} 