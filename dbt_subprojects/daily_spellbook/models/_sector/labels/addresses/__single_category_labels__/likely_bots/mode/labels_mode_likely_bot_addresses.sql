{{
    config(
        alias = 'likely_bot_addresses_mode',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge',
        post_hook = '{{ expose_spells(\'["mode"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('mode') }}