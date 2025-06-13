{{
    config(
        alias = 'likely_bot_contracts',

        post_hook='{{ expose_spells('["bob"]',
        "sector",
        "labels",
        '["msilb7"]') }}'
    )
}}

{{ get_likely_bot_contracts('bob') }} 