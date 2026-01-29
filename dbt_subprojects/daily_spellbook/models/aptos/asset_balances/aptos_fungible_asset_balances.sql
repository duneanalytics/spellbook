{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'unique_key'],
    partition_by = ['block_month']
    , post_hook='{{ hide_spells() }}'
) }}

-- Compared to GraphQL endpoint, this table has the following differences:
-- 1. Historical balances (rather than current balances)
-- 2. When FungibleStore and ObjectCore are not in sync, representation can be different (prior to delete event)
--   a. FungibleStore when ObjectCore is deleted ex 2424873868 idx 8 has no ObjectCore -> will be disallowed later
--   b. ObjectCore when FungibleStore is deleted ex 2975888978 idx 6 has no FungibleStore -> infer from event
-- 3. If an asset has both ConcurrentFungibleBalance and FungibleStore, write_set_change_index is uses former

{% if not is_incremental() -%}
/*
    for historical builds, read from all three tables with no filters
*/
SELECT
    unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    write_set_change_index,
    asset_type,
    owner_address,
    storage_id,
    amount,
    is_frozen,
    token_standard
FROM {{ ref('aptos_fungible_asset_coin_balances') }}

UNION ALL

SELECT
    unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    write_set_change_index,
    asset_type,
    owner_address,
    storage_id,
    amount,
    is_frozen,
    token_standard
FROM {{ ref('aptos_fungible_asset_fa_balances') }}

UNION ALL

SELECT
    unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    write_set_change_index,
    asset_type,
    owner_address,
    storage_id,
    amount,
    is_frozen,
    token_standard
FROM {{ ref('aptos_fungible_asset_fs_deletion_balances') }}

{% else -%}
/*
    for incremental builds, only read from last two tables with block_time filters
*/
SELECT
    unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    write_set_change_index,
    asset_type,
    owner_address,
    storage_id,
    amount,
    is_frozen,
    token_standard
FROM {{ ref('aptos_fungible_asset_fa_balances') }}
WHERE {{ incremental_predicate('block_time') }}

UNION ALL

SELECT
    unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    --
    write_set_change_index,
    asset_type,
    owner_address,
    storage_id,
    amount,
    is_frozen,
    token_standard
FROM {{ ref('aptos_fungible_asset_fs_deletion_balances') }}
WHERE {{ incremental_predicate('block_time') }}
{% endif -%}
