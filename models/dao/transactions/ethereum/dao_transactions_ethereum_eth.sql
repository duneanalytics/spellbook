{{ config(
    
    alias = 'transactions_ethereum_eth',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'dao_creator_tool', 'dao', 'dao_wallet_address', 'tx_hash', 'tx_index', 'tx_type', 'trace_address', 'address_interacted_with', 'value', 'asset_contract_address', 'block_month']
    )
}}

{% set transactions_start_date = '2018-10-27' %}

WITH 

dao_tmp as (
        SELECT 
            blockchain, 
            dao_creator_tool, 
            dao, 
            dao_wallet_address
        FROM 
        {{ ref('dao_addresses_ethereum') }}
        WHERE dao_wallet_address IS NOT NULL
        AND dao_wallet_address NOT IN (0x0000000000000000000000000000000000000001, 0x000000000000000000000000000000000000dead, 0x)
), 

transactions as (
        SELECT 
            block_time, 
            tx_hash, 
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token, 
            value as value, 
            "to" as dao_wallet_address, 
            'tx_in' as tx_type, 
            tx_index,
            COALESCE("from", 0x) as address_interacted_with,
            trace_address
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= DATE '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
        AND "to" IN (SELECT dao_wallet_address FROM dao_tmp)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        AND CAST(value as double) != 0 

        UNION ALL 

        SELECT 
            block_time, 
            tx_hash, 
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token, 
            value as value, 
            "from" as dao_wallet_address, 
            'tx_out' as tx_type,
            tx_index,
            COALESCE("to", 0x) as address_interacted_with,
            trace_address
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= DATE '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' Day)
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
    t.value/POW(10, 18) * p.price as usd_value,
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
    AND p.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p.minute >= DATE '{{transactions_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}