{{
    config(
        alias = 'likely_bot_addresses_bob',
        post_hook='{{ expose_spells(\'["bob"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('bob') }} 