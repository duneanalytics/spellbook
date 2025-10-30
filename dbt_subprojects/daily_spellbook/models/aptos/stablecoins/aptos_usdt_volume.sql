{{ config(
    schema = 'aptos_stablecoins',
    alias = 'usdt_volume',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'event_index'],
    partition_by = ['block_month'],
) }}
-- Augment Tether custom events with mint, burn with fungible_asset_activities
WITH bridge AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        event_index,
        CASE
            WHEN event_type = '0xf73e887a8754f540ee6e1a93bdc6dde2af69fc7ca5de32013e89dd44244473cb::usdt::Mint' THEN 'Mint'
            WHEN event_type = '0xf73e887a8754f540ee6e1a93bdc6dde2af69fc7ca5de32013e89dd44244473cb::usdt::Burn' THEN 'Burn'
        END AS activity_type,
        CAST(json_extract_scalar(data, '$.amount') AS UINT256) AS amount,
        from_hex('0x' || LPAD(LTRIM(SPLIT(json_extract_scalar(data, '$.store.inner'), '::')[1],'0x'),64, '0')) AS fungible_store, -- only in Burn
        from_hex('0x' || LPAD(LTRIM(SPLIT(COALESCE(json_extract_scalar(data, '$.to'), json_extract_scalar(data, '$.from')), '::')[1],'0x'),64, '0')) AS store_owner -- mints to PFS
    FROM {{ source('aptos', 'events') }}
    WHERE 1=1
    AND event_type IN (
        '0xf73e887a8754f540ee6e1a93bdc6dde2af69fc7ca5de32013e89dd44244473cb::usdt::Mint', -- 2495948836
        '0xf73e887a8754f540ee6e1a93bdc6dde2af69fc7ca5de32013e89dd44244473cb::usdt::Burn' -- 1821522020
    )
    {% if is_incremental() or true %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_date >= DATE('2024-10-14')
    {% endif %}
), activity AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        event_index,
        CASE
        WHEN event_type = '0x1::fungible_asset::Withdraw' THEN 'Withdraw'
        WHEN event_type = '0x1::fungible_asset::Deposit' THEN 'Deposit'
        END AS activity_type,
        amount,
        from_hex(storage_id) AS fungible_store,
        owner_address AS store_owner
        -- -- need fab or more parsing to get below info
        -- is_primary,
        -- is_frozen,
    FROM {{ ref('aptos_fungible_asset_activities') }} faa
    WHERE 1=1
    AND asset_type = '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b'
    {% if is_incremental() or true %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_date >= DATE('2024-10-14')
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
    activity_type,
    amount,
    fungible_store,
    store_owner
FROM bridge

UNION ALL

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    event_index,
    activity_type,
    amount,
    fungible_store,
    store_owner
FROM activity
