{{ config(
    schema = 'onepunchswap',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('onepunchswap') }}
