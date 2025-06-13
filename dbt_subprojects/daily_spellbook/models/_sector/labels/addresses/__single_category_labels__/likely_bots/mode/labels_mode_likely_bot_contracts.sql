{{
    config(
        alias = 'likely_bot_contracts_mode',
        post_hook='{{ expose_spells(\'["mode"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('mode') }} 