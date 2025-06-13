{{
    config(
        alias = 'likely_bot_addresses',

        post_hook='{{ expose_spells('["shape"]',
        "sector",
        "labels",
        '["msilb7"]') }}'
    )
}}

{{ get_likely_bot_addresses('shape') }} 