{{ config(
    schema = 'thorchain',
    alias = 'core_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_transfers_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'transfers', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        block_id,
        from_address,
        to_address,
        asset,
        rune_amount,
        rune_amount_usd,
        _unique_key,
        _INSERTED_TIMESTAMP
    FROM
    {{ ref('thorchain_silver_transfers') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_timestamp') }}
    {% endif %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['a._unique_key']
    ) }} AS fact_transfers_id,
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    from_address,
    to_address,
    asset,
    rune_amount,
    rune_amount_usd,
    A._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
    base A
JOIN {{ ref('thorchain_core_block') }} as b
    ON A.block_id = b.block_id
