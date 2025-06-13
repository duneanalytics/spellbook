{{
    config(
        alias = 'likely_bot_contracts_shape',
        post_hook='{{ expose_spells(\'["shape"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('shape') }} 