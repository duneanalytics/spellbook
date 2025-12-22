{{ config(
    schema = 'thorchain',
    alias = 'defi_block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_block_rewards_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'block_rewards', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        day,
        liquidity_fee,
        block_rewards,
        earnings,
        bonding_earnings,
        liquidity_earnings,
        avg_node_count,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_block_rewards') }}
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('day') }}
    {% endif -%}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['a.day']) }} AS fact_block_rewards_id,
    day,
    liquidity_fee,
    block_rewards,
    earnings,
    bonding_earnings,
    liquidity_earnings,
    avg_node_count,
    a._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
  base as a
