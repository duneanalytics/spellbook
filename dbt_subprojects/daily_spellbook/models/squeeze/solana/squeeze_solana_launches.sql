{{ config(
    alias = 'launches',
    schema = 'squeeze_solana',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = 'pool_id'
) }}

{% set project_start_date = '2025-04-15' %}
{% set platform_id = 'FpKUW9vDSRPTByNu4MerR2SU4YPkJU9pLWQTnChGAW3h' %}

-- Squeeze is a Raydium LaunchLab platform (platformId), not a separate program.
-- Filter existing LaunchLab spells by platform_config.

SELECT
    MIN(b.block_time) AS block_time
    , date_trunc('day', MIN(b.block_time)) AS block_date
    , date_trunc('month', MIN(b.block_time)) AS block_month
    , 'solana' AS blockchain
    , 'squeeze' AS project
    , b.pool_id
    , b.base_token_mint AS mint
    , b.quote_token_mint AS quote_mint
    , b.platform_config AS platform_id
    , MAX(b.platform_name) AS platform_name
FROM {{ ref('raydium_launchlab_v1_base_trades') }} b
WHERE b.platform_config = '{{ platform_id }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('b.block_time') }}
  {% else %}
  AND b.block_time >= TIMESTAMP '{{ project_start_date }}'
  {% endif %}
GROUP BY b.pool_id, b.base_token_mint, b.quote_token_mint, b.platform_config
