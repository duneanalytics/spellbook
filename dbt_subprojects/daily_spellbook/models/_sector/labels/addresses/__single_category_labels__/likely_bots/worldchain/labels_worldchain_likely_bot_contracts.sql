{{
    config(
        alias = 'likely_bot_contracts_worldchain',
        post_hook='{{ expose_spells(\'["worldchain"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('worldchain') }} 