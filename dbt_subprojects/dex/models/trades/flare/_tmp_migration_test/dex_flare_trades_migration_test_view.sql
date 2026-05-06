{{ config(
    schema = 'dex_flare_migration_test'
    , alias = 'trades_view'
    , materialized = 'view'
    , tags = ['migration_test', 'prod_exclude']
    )
}}

SELECT * FROM {{ ref('dex_flare_trades_migration_test') }}
