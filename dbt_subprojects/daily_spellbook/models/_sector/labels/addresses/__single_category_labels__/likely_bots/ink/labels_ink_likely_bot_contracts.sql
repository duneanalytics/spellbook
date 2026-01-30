{{
    config(
        alias = 'likely_bot_contracts_ink',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{{ get_likely_bot_contracts('ink') }} 