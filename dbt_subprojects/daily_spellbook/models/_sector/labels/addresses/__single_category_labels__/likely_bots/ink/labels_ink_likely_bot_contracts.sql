{{
    config(
        alias = 'likely_bot_contracts_ink',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge',
        post_hook = '{{ expose_spells(\'["ink"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_contracts('ink') }} 