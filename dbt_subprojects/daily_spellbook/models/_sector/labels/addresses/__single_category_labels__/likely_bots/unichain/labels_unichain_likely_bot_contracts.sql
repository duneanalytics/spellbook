{{
    config(
        alias = 'likely_bot_contracts_unichain',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge',
        post_hook = '{{ expose_spells(\'["unichain"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('unichain') }}