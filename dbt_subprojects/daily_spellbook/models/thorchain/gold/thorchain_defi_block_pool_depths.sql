{{ config(
    schema = 'thorchain',
    alias = 'defi_block_pool_depths',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_depths_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'pool_depths', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        pool_name,
        asset_e8,
        rune_e8,
        synth_e8,
        block_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_block_pool_depths') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['a.pool_name','a.block_timestamp']) }} AS fact_pool_depths_id,
    b.block_timestamp,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    rune_e8,
    asset_e8,
    synth_e8,
    pool_name,
    a._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
    base as a
JOIN {{ ref('thorchain_core_block') }} as b
    ON a.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
OR pool_name IN (
    SELECT
        pool_name
    FROM
        {{ this }}
    WHERE
        dim_block_id = '-1'
)
{% endif %}
