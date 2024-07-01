{{ config(
    schema = 'dao',
    alias = 'transactions_zksync_eth',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'dao_creator_tool', 'dao', 'dao_wallet_address', 'tx_hash', 'tx_index', 'tx_type', 'trace_address', 'address_interacted_with', 'value', 'asset_contract_address', 'block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set transactions_start_date = '2024-06-25' %}

WITH 

dao_tmp as (
        SELECT 
            blockchain, 
            dao_creator_tool, 
            dao, 
            dao_wallet_address
        FROM 
        {{ ref('dao_addresses_zksync') }}
        WHERE dao_wallet_address IS NOT NULL
        AND dao_wallet_address NOT IN (0x0000000000000000000000000000000000000001, 0x000000000000000000000000000000000000dead, 0x)
), 

transactions as (
        SELECT 
            block_time, 
            tx_hash, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token, 
            value as value, 
            "to" as dao_wallet_address, 
            'tx_in' as tx_type, 
            tx_index,
            COALESCE("from", 0x) as address_interacted_with,
            trace_address
        FROM 
        {{ source('zksync', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= DATE '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('block_time')}}
        {% endif %}
        AND "to" IN (SELECT dao_wallet_address FROM dao_tmp)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as double) != 0 

        UNION ALL 

        SELECT 
            block_time, 
            tx_hash, 
            0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as token, 
            value as value, 
            "from" as dao_wallet_address, 
            'tx_out' as tx_type,
            tx_index,
            COALESCE("to", 0x) as address_interacted_with,
            trace_address
        FROM 
        {{ source('zksync', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= DATE '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('block_time')}}
        {% endif %}
        AND "from" IN (SELECT dao_wallet_address FROM dao_tmp)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as double) != 0 
)

SELECT 
    dt.blockchain,
    dt.dao_creator_tool, 
    dt.dao, 
    dt.dao_wallet_address, 
    CAST(date_trunc('day', t.block_time) as DATE) as block_date, 
    CAST(date_trunc('month', t.block_time) as DATE) as block_month, 
    t.block_time, 
    t.tx_type,
    t.token as asset_contract_address,
    'ETH' asset,
    CAST(t.value AS double) as raw_value, 
    t.value/POW(10, 18) as value, 
    t.value/POW(10, 18) * COALESCE(p.price, dp.median_price) as usd_value,
    t.tx_hash, 
    t.tx_index,
    t.address_interacted_with,
    t.trace_address
FROM 
transactions t 
INNER JOIN 
dao_tmp dt 
    ON t.dao_wallet_address = dt.dao_wallet_address
LEFT JOIN 
{{ source('prices', 'usd') }} p 
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.symbol = 'WETH'
    AND p.blockchain = 'zksync'
    {% if not is_incremental() %}
    AND p.minute >= DATE '{{transactions_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= DATE '{{transactions_start_date}}'
    {% endif %}
LEFT JOIN 
{{ source('dex', 'prices') }} dp 
    ON dp.hour = date_trunc('hour', t.block_time)
    AND dp.contract_address = 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91
    AND dp.blockchain = 'zksync'
    AND dp.hour >= DATE '{{transactions_start_date}}'
    {% if is_incremental() %}
    AND {{incremental_predicate('dp.hour')}}
    {% endif %}



