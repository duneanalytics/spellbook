{{ config(
        schema = 'jelly_swap',
        alias = 'trades',
        materialized = 'view'

        , tags = ['prod_exclude']
        )
}}

{{ dex_project_trades('jelly_swap') }}
