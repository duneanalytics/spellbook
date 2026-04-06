{{ config(
    schema = 'spartacus_exchange',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('spartacus_exchange') }}
