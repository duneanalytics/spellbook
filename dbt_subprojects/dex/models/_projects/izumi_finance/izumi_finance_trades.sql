{{ config(
    schema = 'izumi_finance',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{ dex_project_trades('izumi_finance') }}
