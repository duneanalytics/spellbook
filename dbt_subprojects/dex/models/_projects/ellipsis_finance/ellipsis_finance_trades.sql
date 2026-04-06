{{ config(
        schema = 'ellipsis_finance',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'

        , tags = ['prod_exclude']
        )
}}

{{ dex_project_trades('ellipsis_finance') }}
