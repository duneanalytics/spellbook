{{ config(
    schema = 'carbon_defi',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('carbon_defi') }}
