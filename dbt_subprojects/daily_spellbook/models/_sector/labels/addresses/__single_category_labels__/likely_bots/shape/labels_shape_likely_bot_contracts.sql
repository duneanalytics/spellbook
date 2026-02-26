{{
    config(
        alias = 'likely_bot_contracts_shape',
        materialized = 'incremental',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge'
        , tags = ['prod_exclude']
        , post_hook='{{ hide_spells() }}'
    )
}}

{{ get_likely_bot_contracts('shape') }} 