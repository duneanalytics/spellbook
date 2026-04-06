{{ config(
    schema = 'platypus_finance',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('platypus_finance') }}
