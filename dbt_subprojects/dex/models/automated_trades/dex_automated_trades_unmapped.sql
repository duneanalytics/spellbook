{{ config(
    schema = 'dex'
    , alias = 'automated_trades_unmapped'
    , materialized = 'view'
    )
}}

SELECT * 
FROM {{ ref('dex_automated_trades_all') }}
WHERE project_status = false