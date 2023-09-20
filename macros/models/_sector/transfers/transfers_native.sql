{% macro transfers_native(blockchain, traces, transactions, native_token_address, genesis_balances=null, native_erc_transfers=null) %}

WITH

native_transfers  as (
        SELECT 
            'receive' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            to as wallet_address, 
            {{native_token_address}} as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ traces }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND to IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'send' as transfer_type, 
            tx_hash,
            trace_address, 
            block_time,
            "from" as wallet_address, 
            {{native_token_address}} as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ traces }}
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND success
        AND CAST(value as double) > 0
        AND "from" IS NOT NULL 
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),

{% if native_erc_transfers %}
native_erc_transfers as (
        SELECT 
            'deposits' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            to as wallet_address, 
            contract_address as token_address,
            CAST(value as double) as amount_raw
        FROM 
        {{ native_erc_transfers }}
        WHERE contract_address = {{native_token_address}}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}

        UNION ALL 

        SELECT 
            'withdrawals' as transfer_type, 
            evt_tx_hash as tx_hash,
            array[evt_index] as trace_address, 
            evt_block_time as block_time,
            "from" as wallet_address, 
            contract_address as token_address,
            -CAST(value as double) as amount_raw
        FROM 
        {{ native_erc_transfers }}
        WHERE contract_address = {{native_token_address}}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
),
{% endif %}

{% if genesis_balances %}
genesis_balances as (
    SELECT 
        'genesis_balance' as transfer_type, 
        0x as tx_hash, 
        array[-1] as trace_address, 
        legacy_block_time as block_time, 
        address as wallet_address, 
        {{native_token_address}} as token_address,
        balance_raw as amount_raw
    FROM 
    {{ genesis_balances }}
    {% if is_incremental() %}
    WHERE 1 = 0 -- not needed in incremental run 
    {% endif %}
),
{% endif %}

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type,
        hash as tx_hash, 
        array[index] as trace_address, 
        block_time, 
        "from" as wallet_address, 
        {{native_token_address}} as token_address, 
        -(CASE 
            WHEN CAST(gas_price  as double) = 0 THEN 0
            ELSE (CAST(gas_used as DOUBLE) * CAST(gas_price as DOUBLE))
        END) as amount_raw
    FROM 
    {{ transactions }}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
)

SELECT 
    '{{blockchain}}' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
native_transfers

UNION ALL 

SELECT 
    '{{blockchain}}' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
gas_fee

{% if native_erc_transfers %}
UNION ALL 

SELECT 
    '{{blockchain}}' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
native_erc_transfers
{% endif %}

{% if genesis_balances %}
UNION ALL 

SELECT 
    '{{blockchain}}' as blockchain, 
    transfer_type,
    tx_hash, 
    trace_address,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address, 
    token_address, 
    amount_raw
FROM 
genesis_balances
{% endif %}

{% endmacro %}
