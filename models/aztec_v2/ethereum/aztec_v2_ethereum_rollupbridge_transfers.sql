{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'rollupbridge_transfers',
    partition_by = ['evt_block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['bridge_protocol', 'bridge_address', 'tx_from', 'tx_to', 'broad_txn_type', 'to_type', 'from_type', 'bridge_version', 'evt_block_time', 'evt_tx_hash', 'evt_index', 'value_norm', 'contract_address', 'spec_txn_type'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["henrystats"]\') }}'
    )
}}


{% set first_transfer_date = '2022-06-06' %} -- first tx date 

WITH  

all_bridges as (
        SELECT 
            * 
        FROM 
        {{ref('aztec_v2_ethereum_bridges')}}
),

erc20_tfers as (
        SELECT 
            * 
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND `from` IN (SELECT contract_address FROM all_bridges)
        
        UNION 
        
        SELECT 
            * 
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND `to` IN (SELECT contract_address FROM all_bridges)
),

eth_tfers as (
        SELECT 
            * 
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND `from` IN (SELECT contract_address FROM all_bridges)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        
        UNION 

        SELECT 
            * 
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND `to` IN (SELECT contract_address FROM all_bridges)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
), 

tfers_raw as (
        SELECT 
            er.`from` as tx_from, 
            er.`to` as tx_to, 
            er.value, 
            er.contract_address, 
            er.evt_tx_hash, 
            er.evt_index, 
            er.evt_block_time, 
            er.evt_block_number
        FROM 
        erc20_tfers er 
        
        UNION ALL 
        
        SELECT 
            et.`from` as tx_from,
            et.`to` as tx_to, 
            et.value, 
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as contract_address,
            et.tx_hash as evt_tx_hash,
            NULL::bigint as evt_index,
            et.block_time as evt_block_time,
            et.block_number as evt_block_number
        FROM 
        eth_tfers et 
), 

tfers_categorized as (
        SELECT 
            t.*, 
            tk.symbol, 
            tk.decimals, 
            t.value / POW(10, coalesce(tk.decimals, 18)) as value_norm,
            CASE 
                WHEN to_contract.contract_type IS NOT NULL AND from_contract.contract_type IS NOT NULL THEN 'Internal'
                ELSE 'External'        
            END as broad_txn_type,
            CASE 
                WHEN from_contract.contract_type IS NULL AND to_contract.contract_type = 'Rollup' THEN 'User Deposit'
                WHEN to_contract.contract_type IS NULL AND from_contract.contract_type = 'Rollup' THEN 'User Withdrawal'
                WHEN from_contract.contract_type = 'Rollup' AND to_contract.contract_type = 'Bridge' THEN 'RP to Bridge'
                WHEN to_contract.contract_type = 'Rollup' AND from_contract.contract_type = 'Bridge' THEN 'Bridge to RP'
                WHEN from_contract.contract_type = 'Bridge' AND to_contract.contract_type IS NULL THEN 'Bridge to Protocol'
                WHEN to_contract.contract_type = 'Bridge' AND from_contract.contract_type IS NULL THEN 'Protocol to Bridge'
            END as spec_txn_type, 
            to_contract.protocol as to_protocol,
            to_contract.contract_type as to_type,
            from_contract.protocol as from_protocol,
            from_contract.contract_type as from_type,
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.contract_address
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.contract_address
                ELSE NULL
            END as bridge_address,
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.protocol
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.protocol
                ELSE NULL 
            END as bridge_protocol, 
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.version
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.version
                ELSE NULL 
            END as bridge_version,
            date_trunc('day', t.evt_block_time) as evt_block_date -- for partitioning
        FROM tfers_raw t
        LEFT JOIN {{ref('tokens_erc20')}} tk on t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
        LEFT JOIN all_bridges to_contract on t.tx_to = to_contract.contract_address
        LEFT JOIN all_bridges from_contract on t.tx_from = from_contract.contract_address
)
SELECT * FROM tfers_categorized
WHERE value_norm != 0 