{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'activities',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'event_index'],
    partition_by = ['block_month'],
    post_hook='{{ expose_spells(blockchains = \'["aptos"]\',
        spell_type = "project",
        spell_name = "fungible_asset",
        contributors = \'["ying-w"]\') }}'
) }}

-- this query is an approximation of logic behind fungible_asset_activities
-- https://github.com/aptos-labs/aptos-indexer-processors-v2/blob/main/processor/src/processors/fungible_asset/fungible_asset_processor.rs
-- Differences include
-- 1. Gas fees are not included (same as filtering out is_gas_fee)
-- 2. entry_function is not included (join against user_transactions)
-- 3. Excludes freeze events (uncommon) `0x1::fungible_asset::Frozen`

{% if not is_incremental() -%}
/*
    for historical builds, read from all three tables with 7-day filter
*/
SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    event_index,
    event_type,
    owner_address,
    storage_id,
    asset_type,
    amount,
    token_standard
FROM {{ ref('aptos_fungible_asset_coin_activities') }}
WHERE block_time >= DATE('2025-08-02') - INTERVAL '7' DAY

UNION ALL

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    event_index,
    event_type,
    owner_address,
    storage_id,
    asset_type,
    amount,
    token_standard
FROM {{ ref('aptos_fungible_asset_fa_activities_events_v1') }}
WHERE block_time >= DATE('2024-05-29') - INTERVAL '7' DAY

UNION ALL

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    event_index,
    event_type,
    owner_address,
    storage_id,
    asset_type,
    amount,
    token_standard
FROM {{ ref('aptos_fungible_asset_fa_activities') }}
WHERE block_time >= NOW() - INTERVAL '7' DAY

{% else -%}
/*
    for incremental builds, only read from fa_activities with block_time filter
*/

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    event_index,
    event_type,
    owner_address,
    storage_id,
    asset_type,
    amount,
    token_standard
FROM {{ ref('aptos_fungible_asset_fa_activities') }}
WHERE {{ incremental_predicate('block_time') }}
{% endif -%}
