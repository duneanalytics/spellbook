{{
    config(
        alias = 'likely_bot_addresses_unichain',
        post_hook='{{ expose_spells(\'["unichain"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('unichain') }}