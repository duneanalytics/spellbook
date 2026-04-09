{{ config(
        schema = 'beethoven_x',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{{ dex_project_trades('beethoven_x') }}
