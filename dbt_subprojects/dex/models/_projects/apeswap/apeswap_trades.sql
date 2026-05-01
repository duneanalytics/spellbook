{{ config(
        schema = 'apeswap',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{{ dex_project_trades('apeswap') }}
