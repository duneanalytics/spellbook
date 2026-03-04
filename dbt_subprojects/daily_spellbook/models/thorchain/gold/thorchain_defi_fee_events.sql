{{ config(
    schema = 'thorchain',
    alias = 'defi_fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'fee_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,  -- Convert to FlipsideCrypto naming
    asset,
    pool_deduct,
    asset_e8,
    event_id,
    raw_block_timestamp as block_timestamp,  -- Use raw timestamp for exact JOIN
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_fee_events') }}
)

SELECT
  a.block_time,
  date(a.block_time) as block_date,
  date_trunc('month', a.block_time) as block_month,
  -1 AS block_height,
  a.tx_id,
  a.event_id,
  a.asset,
  a.pool_deduct,
  a.asset_e8,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}
