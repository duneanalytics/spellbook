{{ config(
        schema = 'ellipsis_finance',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{{ dex_project_trades('ellipsis_finance') }}
