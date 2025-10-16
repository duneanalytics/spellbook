{{ config(
    schema = 'thorchain_silver',
    alias = 'fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'fees']
) }}

with base as (
    SELECT
        tx as tx_hash,
        asset,
        asset_e8 / 1e8 as asset_amount,
        asset_e8,
        pool_deduct / 1e8 as pool_deduct_amount,
        pool_deduct,
        event_id,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        block_timestamp as raw_block_timestamp,
        
        CASE 
            WHEN asset LIKE 'THOR.%' THEN cast(null as varbinary)
            ELSE cast(asset as varbinary)
        END as contract_address,
        
        CASE 
            WHEN asset LIKE '%.%' THEN split_part(asset, '.', 1)
            ELSE 'THOR'
        END as asset_chain,
        
        CASE 
            WHEN asset LIKE '%.%' THEN split_part(asset, '.', 2)
            ELSE asset
        END as asset_symbol

    FROM {{ source('thorchain', 'fee_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
)

SELECT * FROM base
