{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'fa_activities_events_v1',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_month'],
    tags = ['static']
) }}

/*
    - this model has a cutoff date of 2024-05-29 to exclude activities after the Events v2 migration
    - leverage static tag to only run on initial build
*/

WITH fa_activities_legacy AS (
    -- FA activities prior to Events v2 migration
    SELECT
        ev.tx_version,
        ev.tx_hash,
        ev.block_date,
        ev.block_time,
        --
        event_index,
        event_type,
        guid_account_address, -- storage_id
        CAST(json_extract_scalar(data, '$.amount') AS uint256) AS amount,
        fab.asset_type,
        fab.owner_address
    FROM {{ source('aptos', 'events') }} ev
    LEFT JOIN {{ ref('aptos_fungible_asset_balances') }} fab -- TODO: edge case around deletes
        ON ev.tx_version = fab.tx_version
        AND ev.guid_account_address = fab.storage_id
        AND fab.token_standard = 'v2'
        AND fab.block_date >= DATE('2023-07-28')
        AND fab.block_date <= DATE('2024-05-29')
    WHERE 1=1
        AND ev.block_date >= DATE('2023-07-28') -- FA deployed
        AND ev.block_date <= DATE('2024-05-29') -- Events v2 migration completed
        AND event_type IN (
            '0x1::fungible_asset::DepositEvent',
            '0x1::fungible_asset::WithdrawEvent'
        )
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
    guid_account_address AS storage_id,
    asset_type,
    amount,
    'v2' AS token_standard
FROM fa_activities_legacy
