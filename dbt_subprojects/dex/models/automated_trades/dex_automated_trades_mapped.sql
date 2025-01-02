{{ config(
    schema = 'dex'
    , alias = 'automated_trades_mapped'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'view'
    )
}}

SELECT *
FROM {{ ref('dex_automated_trades_all') }}
WHERE project_status = 'true' 