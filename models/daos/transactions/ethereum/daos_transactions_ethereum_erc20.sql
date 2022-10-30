{{ config(
    alias = 'daos_transactions_ethereum_erc20',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'dao_creator_tool', 'dao', 'dao_wallet_address', 'tx_hash', 'tx_index', 'token', 'tx_type', 'trace_address', 'address_interacted_with']
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
        {{ ref('daos_addresses_ethereum') }}
), 

transactions as (
        SELECT 
            evt_block_time as block_time, 
            evt_tx_hash as tx_hash, 
            contract_address as token, 
            value as value, 
            to as dao_wallet_address, 
            'tx_in' as tx_type,
            evt_index as tx_index,
            from as address_interacted_with,
            array('') as trace_address
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND to IN (SELECT dao_wallet_address FROM dao_tmp)

        UNION ALL 

        SELECT 
            evt_block_time as block_time, 
            evt_tx_hash as tx_hash, 
            contract_address as token, 
            value as value, 
            from as dao_wallet_address, 
            'tx_out' as tx_type, 
            evt_index as tx_index, 
            to as address_interacted_with,
            array('') as trace_address
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND from IN (SELECT dao_wallet_address FROM dao_tmp)
)

SELECT 
    dt.blockchain,
    dt.dao_creator_tool, 
    dt.dao, 
    dt.dao_wallet_address, 
    TRY_CAST(date_trunc('day', t.block_time) as DATE) as block_date, 
    t.block_time, 
    t.tx_type,
    COALESCE(er.symbol, t.token) as asset,
    t.value as raw_value, 
    t.value/POW(10, COALESCE(er.decimals, 18)) as value, 
    t.value/POW(10, COALESCE(er.decimals, 18)) * p.price as usd_value, 
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
{{ ref('tokens_erc20') }} er 
    ON t.token = er.contract_address
    AND er.blockchain = 'ethereum'
LEFT JOIN 
{{ source('prices', 'usd') }} p 
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.contract_address = t.token
    AND p.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p.minute >= '{{transactions_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}

