{{ config(
    schema = 'mummy_finance',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('mummy_finance') }}
