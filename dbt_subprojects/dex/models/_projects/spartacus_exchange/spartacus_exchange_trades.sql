{{ config(
    schema = 'spartacus_exchange',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'

    , tags = ['prod_exclude']
    )
}}

{{ dex_project_trades('spartacus_exchange') }}
