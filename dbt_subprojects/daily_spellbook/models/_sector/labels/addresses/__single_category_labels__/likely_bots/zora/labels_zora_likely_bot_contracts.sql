{{
    config(
        alias = 'likely_bot_contracts_zora',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
        incremental_strategy = 'merge'
        , tags = ['prod_exclude', 'static']
        , post_hook='{{ hide_spells() }}'
    )
}}

{{ get_likely_bot_contracts('zora') }} 