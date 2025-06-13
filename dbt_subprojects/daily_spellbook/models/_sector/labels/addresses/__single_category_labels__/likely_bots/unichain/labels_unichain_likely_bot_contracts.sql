{{
    config(
        alias = 'likely_bot_contracts_unichain',
        post_hook='{{ expose_spells(\'["unichain"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('unichain') }}