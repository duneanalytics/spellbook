{{ config(
    schema = 'aptos_stablecoins',
    alias = 'usdc_volume',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'event_index'],
    partition_by = ['block_month'],
) }}
-- Circle uses custom events for mint, burn, withdraw, deposit
-- however, this is not comprehensive and need to also use data from bridge
WITH bridge AS (
    SELECT
        tx_version,
        event_index,
        CAST(json_extract_scalar(data, '$.amount') AS UINT256) AS amount,
        CASE
            WHEN event_type = '0x9bce6734f7b63e835108e3bd8c36743d4709fe435f44791918801d0989640a9d::token_messenger::DepositForBurn'
            THEN '0x' || LPAD(LTRIM(SPLIT(json_extract_scalar(data, '$.depositor'), '::')[1],'0x'),64, '0')
            WHEN event_type = '0x9bce6734f7b63e835108e3bd8c36743d4709fe435f44791918801d0989640a9d::token_messenger::MintAndWithdraw'
            THEN '0x' || LPAD(LTRIM(SPLIT(json_extract_scalar(data, '$.mint_recipient'), '::')[1],'0x'),64, '0')
        END AS store_owner,
        -- only for burn
        CAST(json_extract_scalar(data, '$.destination_domain') AS INT) AS destination_domain,
        json_extract_scalar(data, '$.mint_recipient') AS mint_recipient
    -- FROM {{ source('aptos', 'events') }}
    FROM aptos.events
    WHERE 1=1
    AND event_type IN (
        '0x9bce6734f7b63e835108e3bd8c36743d4709fe435f44791918801d0989640a9d::token_messenger::DepositForBurn', -- 2360646121
        '0x9bce6734f7b63e835108e3bd8c36743d4709fe435f44791918801d0989640a9d::token_messenger::MintAndWithdraw' -- 2359912971
    )
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_date >= DATE('2025-01-02')
    {% endif %}
), activity AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        event_index,
        CASE
            WHEN event_type = '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::treasury::Mint' THEN 'Mint'
            WHEN event_type = '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::treasury::Burn' THEN 'Burn'
            WHEN event_type = '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::stablecoin::Withdraw' THEN 'Withdraw'
            WHEN event_type = '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::stablecoin::Deposit' THEN 'Deposit'
        END AS activity_type,
        data,
        CAST(json_extract_scalar(data, '$.amount') AS UINT256) AS amount,
        -- https://developers.circle.com/stablecoins/aptos-packages
        -- burner and minter often fixed to 0x9e6702a472080ea3caaf6ba9dfaa6effad2290a9ba9adaacd5af5c618e42782d
        -- '0x' || LPAD(LTRIM(SPLIT(COALESCE(json_extract_scalar(data, '$.minter'), json_extract_scalar(data, '$.burner')), '::')[1],'0x'),64, '0') AS source_address, -- TokenMessengerMinter 
        from_hex('0x' || LPAD(LTRIM(SPLIT(json_extract_scalar(data, '$.store'), '::')[1],'0x'),64, '0')) AS fungible_store,
        from_hex('0x' || LPAD(LTRIM(SPLIT(json_extract_scalar(data, '$.store_owner'), '::')[1],'0x'),64, '0')) AS store_owner
    FROM {{ source('aptos', 'events') }}
    WHERE 1=1
    AND event_type IN (
        '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::treasury::Mint', -- 2473123058 no bridge
        '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::treasury::Burn', -- 2403163407 no bridge
        '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::stablecoin::Withdraw',
        '0xe5c5befe31ce06bc1f2fd31210988aac08af6d821b039935557a6f14c03471be::stablecoin::Deposit'
    )
    AND json_extract_scalar(data, '$.amount') != '0'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_date >= DATE('2024-12-19')
    {% endif %}
), final AS (
    SELECT
        activity.block_date,
        activity.block_time,
        activity.tx_version,
        activity.tx_hash,
        activity.event_index,
        activity.activity_type,
        activity.amount,
        COALESCE(activity.fungible_store, m.fungible_store, b.fungible_store) AS fungible_store,
        COALESCE(activity.store_owner, m.store_owner, b.store_owner, bridge.store_owner) AS store_owner
    FROM activity
    LEFT JOIN ( -- to catch non-bridge mints
        SELECT * FROM activity WHERE activity_type = 'Deposit'
    ) m
    ON activity.tx_version = m.tx_version
    AND activity.amount = m.amount
    AND activity.activity_type = 'Mint'
    AND (activity.event_index = m.event_index - 1 OR activity.event_index = m.event_index - 2)
    LEFT JOIN ( -- to catch non-bridge burn
        SELECT * FROM activity WHERE activity_type = 'Withdraw'
    ) b
    ON activity.tx_version = b.tx_version
    AND activity.amount = b.amount
    AND activity.activity_type = 'Burn'
    AND activity.event_index = b.event_index + 1
    -- above burn logic will miss
    -- swaps ex. 2668992625
    -- take fee ex. 2678815051
    LEFT JOIN bridge -- fallback
    ON activity.tx_version = bridge.tx_version
    AND activity.amount = bridge.amount
    AND bridge.event_index - activity.event_index <= 3 -- 3 for mint, 2 for burn
    AND activity.activity_type IN ('Mint', 'Burn')
    WHERE 1=1
    -- AND activity.tx_version = 2671802709
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
    fungible_store, -- storage_id
    store_owner
FROM final
WHERE 1=1
