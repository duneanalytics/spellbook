{{ config(
    
    schema = 'aztec_v2_ethereum',
    alias = 'rollupbridge_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}'
    )
}}


{% set first_transfer_date = '2022-06-06' %} -- first tx date 

WITH  

bridges_label (protocol, version, description, contract_address) as (
        VALUES 
            ('Aztec RollupProcessor', '1.0', 'Prod Aztec Rollup', 0xff1f2b4adb9df6fc8eafecdcbf96a2b351680455),
            ('Element', '1.0', 'Prod Element Bridge', 0xaed181779a8aabd8ce996949853fea442c2cdb47),
            ('Lido', '1.0', 'Prod Lido Bridge', 0x381abf150b53cc699f0dbbbef3c5c0d1fa4b3efd),
            ('AceofZk', '1.0', 'Ace Of ZK NFT - nonfunctional', 0x0eb7f9464060289fe4fddfde2258f518c6347a70),
            ('Curve', '1.0', 'CurveStEth Bridge', 0x0031130c56162e00a7e9c01ee4147b11cbac8776),
            ('Yearn', '1.0', 'Yearn Deposits', 0xe71a50a78cccff7e20d8349eed295f12f0c8c9ef),
            ('Aztec', '1.0', 'ERC4626 Tokenized Vault', 0x3578d6d5e1b4f07a48bb1c958cbfec135bef7d98),
            ('Curve', '1.0', 'CurveStEth Bridge V2', 0xe09801da4c74e62fb42dfc8303a1c1bd68073d1a),
            ('Uniswap', '1.0', 'UniswapDCABridge', 0x94679a39679ffe53b53b6a1187aa1c649a101321)
), 

bridges_creation as (
        SELECT 
            bridgeAddress, 
            'Bridge' as contract_type, 
            AVG(bridgeGasLimit) as blank -- to get unique bridges 
        FROM 
        {{source('aztec_v2_ethereum', 'RollupProcessor_evt_BridgeAdded')}}
        GROUP BY 1, 2 
        
        UNION 
        
        SELECT 
            0xFF1F2B4ADb9dF6FC8eAFecDcbF96A2B351680455 as bridgeAddress, 
            'Rollup' as contract_type,
            100 as blank 
),

all_bridges as (

        SELECT 
            bl.protocol,
            bl.version,
            bl.description, 
            bc.contract_type,
            bc.bridgeAddress as contract_address 
        FROM 
        bridges_creation bc 
        LEFT JOIN 
        bridges_label bl 
            ON bl.contract_address = bc.bridgeAddress

), 

erc20_tfers as (
        SELECT 
            * 
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= TIMESTAMP '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        AND "from" IN (SELECT contract_address FROM all_bridges)
        
        UNION 
        
        SELECT 
            * 
        FROM 
        {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= TIMESTAMP '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        AND "to" IN (SELECT contract_address FROM all_bridges)
),

eth_tfers as (
        SELECT 
            * 
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= TIMESTAMP '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        AND "from" IN (SELECT contract_address FROM all_bridges)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
        
        UNION 

        SELECT 
            * 
        FROM 
        {{ source('ethereum', 'traces') }}
        {% if not is_incremental() %}
        WHERE block_time >= TIMESTAMP '{{first_transfer_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        AND "to" IN (SELECT contract_address FROM all_bridges)
        AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type IS NULL)
        AND success = true 
), 

tfers_raw as (
        SELECT 
            er."from" as tx_from, 
            er."to" as tx_to, 
            er.value, 
            er.contract_address, 
            er.evt_tx_hash, 
            er.evt_index, 
            er.evt_block_time, 
            er.evt_block_number,
            CAST(ARRAY[-1] as array<bigint>) as trace_address
        FROM 
        erc20_tfers er 
        
        UNION ALL 
        
        SELECT 
            et."from" as tx_from,
            et."to" as tx_to, 
            et.value, 
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as contract_address,
            et.tx_hash as evt_tx_hash,
            et.tx_index as evt_index,
            et.block_time as evt_block_time,
            et.block_number as evt_block_number,
            et.trace_address
        FROM 
        eth_tfers et 
), 

tfers_categorized as (
        SELECT 
            t.*, 
            date_trunc('day', t.evt_block_time) as evt_block_date,
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
            COALESCE(to_contract.protocol, '') as to_protocol,
            COALESCE(to_contract.contract_type, '') as to_type,
            COALESCE(from_contract.protocol, '') as from_protocol,
            COALESCE(from_contract.contract_type, '') as from_type,
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.contract_address
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.contract_address
                ELSE CAST(NULL AS VARBINARY)
            END as bridge_address,
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.protocol
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.protocol
                ELSE ''
            END as bridge_protocol, 
            CASE 
                WHEN to_contract.contract_type = 'Bridge' THEN to_contract.version
                WHEN from_contract.contract_type = 'Bridge' THEN from_contract.version
                ELSE ''
            END as bridge_version
        FROM tfers_raw t
        LEFT JOIN {{source('tokens', 'erc20')}}  tk on t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
        LEFT JOIN all_bridges to_contract on t.tx_to = to_contract.contract_address
        LEFT JOIN all_bridges from_contract on t.tx_from = from_contract.contract_address
) 
SELECT * FROM tfers_categorized
WHERE value_norm != 0