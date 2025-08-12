WITH latest_metadata AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY asset_type ORDER BY block_time DESC) AS rn
    FROM {{ ref('fungible_asset_metadata') }}
)

SELECT
    block_date,
    tx_version,
    block_time,
    block_month,
    tx_hash,
    --
    write_set_change_index,
    asset_type,
    owner_address,
    asset_name,
    asset_symbol,
    decimals,
    supply,
    --
    token_standard,
    asset_type_migrated,
    icon_uri,
    project_uri,
    maximum
FROM latest_metadata
WHERE rn = 1 -- no easy way to exclude
