{{
    config(
        alias = 'likely_bot_addresses_shape',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge',
        post_hook = '{{ expose_spells(\'["shape"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('shape') }} 