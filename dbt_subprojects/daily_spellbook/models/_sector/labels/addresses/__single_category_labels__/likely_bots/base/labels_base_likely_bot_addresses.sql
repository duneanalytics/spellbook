{{
    config(
        alias = 'likely_bot_addresses_base',
        post_hook='{{ expose_spells(\'["base"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('base') }}