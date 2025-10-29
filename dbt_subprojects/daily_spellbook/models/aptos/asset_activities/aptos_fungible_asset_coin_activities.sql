{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'coin_activities',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_month'],
    tags = ['static']
) }}

/*
    - this model has a cutoff date of 2025-08-02 to exclude activities after the FS migration
    - leverage static tag to only run on initial build
*/

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
            AND block_date <= DATE('2025-08-02') -- FS migration
            AND block_date >= DATE('2025-07-25') -- beginning of FA (v2) migration
    ) mr
        ON ev.tx_version = mr.tx_version
        AND ev.block_date = mr.block_date -- optimization
        AND ev.guid_account_address = mr.addr
        AND ev.guid_creation_number = mr.creation_num
    WHERE 1=1
        AND event_type IN ('0x1::coin::WithdrawEvent', '0x1::coin::DepositEvent')
        AND ev.block_date <= DATE('2025-08-02') -- FS migration
        AND ev.block_date >= DATE('2025-07-25') -- beginning of FA (v2) migration
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
    CAST(NULL AS varbinary) AS storage_id,
    asset_type,
    amount,
    'v1' AS token_standard
FROM coin_activities
