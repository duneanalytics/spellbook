{{ config(
    schema = 'spiritswap',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('spiritswap') }}
