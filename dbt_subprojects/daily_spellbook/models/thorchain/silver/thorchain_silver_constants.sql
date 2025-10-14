{{ config(
    schema = 'thorchain_silver',
    alias = 'constants',
    materialized = 'view',
    tags = ['thorchain', 'constants', 'silver', 'reference']
) }}

-- Simple passthrough view for Thorchain constants (key-value lookup table)
SELECT
    key,
    value
FROM {{ source('thorchain', 'constants') }}

