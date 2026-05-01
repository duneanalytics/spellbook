{{ config(
        schema = 'lifi',
        alias = 'trades'
        , post_hook='{{ hide_spells() }}'
        )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'lifi'