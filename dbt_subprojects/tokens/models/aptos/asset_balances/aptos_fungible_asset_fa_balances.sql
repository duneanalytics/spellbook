{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'fa_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'unique_key'],
    partition_by = ['block_month']
) }}

WITH fa_balance AS (
    -- FungibleStore (balance), ConcurrentFungibleBalance (preferred balance when present) and the
    -- owning Object resource share (tx_version, move_address) within a tx. Scan move_resources ONCE
    -- and fan the per-resource columns out with conditional aggregation in a single GROUP BY,
    -- instead of scanning the table 3x and LEFT JOINing on (tx_version, move_address) -- Trino does
    -- not share repeated scans. The HAVING keeps only keys with the driving FungibleStore row.
    -- (owner is NULL if the object was deleted in this tx, same as before.)
    SELECT
        tx_version,
        move_address,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then tx_hash end) AS tx_hash,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then block_date end) AS block_date,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then block_time end) AS block_time,
        --
        COALESCE(
            max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'ConcurrentFungibleBalance' then write_set_change_index end),
            max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then write_set_change_index end)
        ) AS write_set_change_index,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then json_extract_scalar(move_data, '$.metadata.inner') end) AS asset_type,
        max(case when move_resource_module = 'object' and move_resource_name IN ('ObjectGroup', 'ObjectCore')
            then '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.owner'), '0x'), 64, '0') end) AS owner_address,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then move_is_deletion end) AS move_is_deletion,
        COALESCE(
            max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'ConcurrentFungibleBalance' then json_extract_scalar(move_data, '$.balance.value') end), -- ConcurrentFungibleBalance
            max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then json_extract_scalar(move_data, '$.balance') end) -- FungibleStore
        ) AS balance,
        max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then CAST(json_extract_scalar(move_data, '$.frozen') AS BOOLEAN) end) AS is_frozen
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND (
            (move_resource_module = 'fungible_asset' AND move_resource_name IN ('FungibleStore', 'ConcurrentFungibleBalance'))
            OR (move_resource_module = 'object' AND move_resource_name IN ('ObjectGroup', 'ObjectCore'))
        )
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% endif -%}
        {% if target.name == 'ci' %}
        -- bound the full-refresh scan in CI so the build completes and the regression test runs; prod is unaffected
        AND block_date >= current_date - interval '14' day
        {% endif %}
    GROUP BY tx_version, move_address
    HAVING max(case when move_resource_module = 'fungible_asset' and move_resource_name = 'FungibleStore' then 1 else 0 end) = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'write_set_change_index']) }} AS unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    write_set_change_index,
    '0x' || LPAD(LTRIM(asset_type, '0x'), 64, '0') AS asset_type,
    from_hex(owner_address) AS owner_address,
    move_address AS storage_id,
    CAST(IF(move_is_deletion, '0', balance) AS UINT256) AS amount,
    is_frozen,
    'v2' AS token_standard
FROM fa_balance