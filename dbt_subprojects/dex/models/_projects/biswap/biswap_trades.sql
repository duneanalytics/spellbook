{{ config(
    schema = 'biswap',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('biswap') }}
