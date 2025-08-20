-- this query is an approximation of logic behind fungible_asset_activities (does not include gas fees or entryfn)
-- https://github.com/aptos-labs/aptos-indexer-processors-v2/blob/main/processor/src/processors/fungible_asset/fungible_asset_processor.rs

{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'activities',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'event_index'],
    partition_by = ['block_month'],
) }}

WITH coin_activities AS (
    -- example to check: tx_version = 1887949456 '2024-11-05'
    SELECT
        ev.tx_version,
        tx_hash,
        ev.block_date,
        block_time,
        --
        event_index,
        event_type,
        mr.asset_type,
        guid_account_address,
        CAST(json_extract_scalar(data, '$.amount') AS uint256) AS amount
    FROM {{ source('aptos', 'events') }} ev
    INNER JOIN (
        SELECT
            tx_version,
            block_date,
            move_resource_generic_type_params[1] AS asset_type,
            event_data['coin_event'] AS coin_event,
            address_32_from_hex(event_data['addr']) AS addr,
            CAST(event_data['creation_num'] AS uint256) AS creation_num
        FROM {{ source('aptos', 'move_resources') }},
        UNNEST(
            ARRAY[
            MAP(ARRAY['coin_event', 'addr', 'creation_num'], ARRAY['deposit', json_extract_scalar(move_data, '$.deposit_events.guid.id.addr'), json_extract_scalar(move_data, '$.deposit_events.guid.id.creation_num')]),
            MAP(ARRAY['coin_event', 'addr', 'creation_num'], ARRAY['withdraw', json_extract_scalar(move_data, '$.withdraw_events.guid.id.addr'), json_extract_scalar(move_data, '$.withdraw_events.guid.id.creation_num')])
            ]
        ) AS t (event_data)
        WHERE 1=1
            AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
            AND move_resource_module = 'coin'
            AND move_resource_name = 'CoinStore'
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
    ) mr
        ON ev.tx_version = mr.tx_version
        AND ev.block_date = mr.block_date
        AND ev.guid_account_address = mr.addr
        AND ev.guid_creation_number = mr.creation_num
    WHERE 1=1
        AND event_type IN ('0x1::coin::WithdrawEvent', '0x1::coin::DepositEvent')
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
), fa_activities AS (
    SELECT
        ev.tx_version,
        ev.tx_hash,
        ev.block_date,
        ev.block_time,
        --
        event_index,
        event_type,
        '0x' || LPAD(LTRIM(json_extract_scalar(data, '$.store'), '0x'), 64, '0') AS storage_id,
        CAST(json_extract_scalar(data, '$.amount') AS uint256) AS amount,
        fab.owner_address,
        fab.asset_type
    FROM {{ source('aptos', 'events') }} ev
    LEFT JOIN {{ ref('aptos_fungible_asset_balances') }} fab -- TODO: edge case around deletes
    ON ev.tx_version = fab.tx_version
    AND address_32_from_hex(json_extract_scalar(ev.data, '$.store')) = fab.storage_id
    AND fab.token_standard = 'v2'
    {% if is_incremental() %}
    AND {{ incremental_predicate('fab.block_time') }}
    {% endif %}
    WHERE 1=1
        AND ev.block_date >= DATE('2023-07-28') -- v2 deployed
        AND event_type IN (
            '0x1::fungible_asset::Deposit',
            '0x1::fungible_asset::Withdraw'
        )
    {% if is_incremental() %}
        AND {{ incremental_predicate('ev.block_time') }}
    {% endif %}
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
    guid_account_address AS owner_address,
    NULL AS storage_id,
    asset_type,
    amount,
    'v1' AS token_standard
FROM coin_activities

UNION ALL

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
