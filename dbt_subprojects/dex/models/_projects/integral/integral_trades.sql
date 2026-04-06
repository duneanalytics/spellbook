{{ config(
        schema = 'integral',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'

        , tags = ['prod_exclude']
        )
}}

{{ dex_project_trades('integral') }}
