{{
    config(
        alias = 'likely_bot_contracts_base',
        post_hook='{{ expose_spells(\'["base"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('base') }}