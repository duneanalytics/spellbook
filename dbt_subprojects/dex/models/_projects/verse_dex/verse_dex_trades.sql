{{ config(
    schema = 'verse_dex',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'

    , tags = ['prod_exclude']
    )
}}

{{ dex_project_trades('verse_dex') }}
