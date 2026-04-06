{{ config(
    schema = 'verse_dex',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('verse_dex') }}
