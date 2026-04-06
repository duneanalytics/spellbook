{{ config(
        schema = 'trader_joe',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

{{ dex_project_trades('trader_joe') }}
