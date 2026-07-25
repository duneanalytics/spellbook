{{ config(
    alias = 'deployments',
    schema = 'squeeze',
    materialized = 'view'
) }}

SELECT * FROM {{ ref('squeeze_base_deployments') }}
UNION ALL
SELECT * FROM {{ ref('squeeze_robinhood_deployments') }}
