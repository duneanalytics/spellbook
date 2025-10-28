{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'fa_activities',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'event_index'],
    partition_by = ['block_month']
) }}

-- this query is an approximation of logic behind fungible_asset_activities
-- https://github.com/aptos-labs/aptos-indexer-processors-v2/blob/main/processor/src/processors/fungible_asset/fungible_asset_processor.rs
-- Differences include
-- 1. Gas fees are not included (same as filtering out is_gas_fee)
-- 2. entry_function is not included (join against user_transactions)
-- 3. Excludes freeze events (uncommon) `0x1::fungible_asset::Frozen`

{% if is_incremental() -%}
WITH max_fab_version AS (
    SELECT MAX(tx_version) AS max_tx_version
    FROM {{ ref('aptos_fungible_asset_balances') }}
)
, fa_activities AS (
{% else -%}
WITH fa_activities AS (
{% endif -%}
    SELECT
        ev.tx_version,
        ev.tx_hash,
        ev.block_date,
        ev.block_time,
        --
        event_index,
        event_type,
        address_32_from_hex('0x' || LPAD(LTRIM(json_extract_scalar(data, '$.store'), '0x'), 64, '0')) AS storage_id,
        CAST(json_extract_scalar(data, '$.amount') AS uint256) AS amount,
        fab.owner_address,
        fab.asset_type
    FROM {{ source('aptos', 'events') }} ev
    LEFT JOIN {{ ref('aptos_fungible_asset_balances') }} fab
        ON ev.tx_version = fab.tx_version
        AND address_32_from_hex(json_extract_scalar(ev.data, '$.store')) = fab.storage_id
        AND fab.token_standard = 'v2'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('fab.block_time') }}
        {% endif -%}
    {% if is_incremental() -%}
    CROSS JOIN max_fab_version
    {% endif -%}
    WHERE 1=1
        AND ev.block_date >= DATE('2024-05-29')
        AND event_type IN (
            '0x1::fungible_asset::Deposit',
            '0x1::fungible_asset::Withdraw'
        )
        {% if is_incremental() -%}
        AND {{ incremental_predicate('ev.block_time') }}
        AND ev.tx_version < max_fab_version.max_tx_version
        {% endif -%}
)

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    event_index,
    event_type,
    owner_address,
    storage_id,
    asset_type,
    amount,
    'v2' AS token_standard
FROM fa_activities