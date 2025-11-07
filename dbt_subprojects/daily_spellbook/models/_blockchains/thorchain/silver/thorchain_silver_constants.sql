{{ config(
    schema = 'thorchain_silver',
    alias = 'constants',
    materialized = 'view',
    tags = ['thorchain', 'constants', 'silver', 'reference']
) }}

SELECT
    key,
    value
FROM {{ source('thorchain', 'constants') }}

