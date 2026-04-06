{{ config(
        schema = 'jelly_swap',
        alias = 'trades',
        materialized = 'view'
        )
}}

{{ dex_project_trades('jelly_swap') }}
