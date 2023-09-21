{% macro transfers_native(blockchain, traces, transactions, native_token_address, genesis_balances=null, native_erc_transfers=null, suicide_self_destruct=null, contract_creation_deposit=null) %}

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
        AND to != 0x0000000000000000000000000000000000000000
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
        AND "from" != 0x0000000000000000000000000000000000000000 
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
        CAST(balance_raw as double) as amount_raw
    FROM 
    {{ genesis_balances }}
    {% if is_incremental() %}
    WHERE 1 = 0 -- not needed in incremental run 
    {% endif %}
),
{% endif %}

{% if suicide_self_destruct %}
suicide_creations as (
    SELECT 
        "from" as suicide_contract, 
        tx_index,
        address as contract_address, 
        tx_hash
    FROM 
    {{ traces }}
    WHERE type = 'create'
    AND call_type IS NULL -- helpful filter
    AND refund_address IS NULL -- filter
    AND value = UINT256 '0'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

suicide_join_logs as (
    SELECT 
        el.tx_hash, 
        el.tx_index, 
        el.block_time, 
        bytearray_substring(el.data, 13, 20) as suicide_contract_address, 
        bytearray_substring(el.data, 45, 20) as suicide_refund_address, 
        bytearray_to_uint256(bytearray_substring(el.data,65,32)) as amount_raw 
    FROM 
    {{ suicide_self_destruct }} el 
    INNER JOIN 
    suicide_creations sc 
        ON el.contract_address = sc.suicide_contract
        AND bytearray_substring(el.data, 13, 20) = sc.contract_address
        AND el.tx_hash = sc.tx_hash 
        AND el.tx_index = sc.tx_index
    WHERE el.topic0 IS NULL 
    AND bytearray_to_uint256(bytearray_substring(el.data,65,32)) > UINT256 '0'
    {% if is_incremental() %}
    AND el.block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

suicide_deposits_withdrawals as (
    SELECT 
        'suicide_refund' as transfer_type, 
        tx_hash, 
        array[tx_index] as trace_address, 
        block_time, 
        suicide_refund_address as wallet_address,
        {{native_token_address}} as token_address, 
        CAST(amount_raw as double) as amount_raw 
    FROM 
    suicide_join_logs

    UNION ALL 

    SELECT 
        'suicide_self_destruct' as transfer_type, 
        tx_hash, 
        array[tx_index] as trace_address, 
        block_time, 
        suicide_contract_address as wallet_address,
        {{native_token_address}} as token_address, 
        -(CAST(amount_raw as double)) as amount_raw 
    FROM 
    suicide_join_logs
),
{% endif %}

{% if contract_creation_deposit %}
contract_creation_deposit as (
    SELECT 
        'contract_creation_deposit' as transfer_type, 
        tx_hash, 
        array[tx_index] as trace_address,
        block_time, 
        address as wallet_address,
        {{native_token_address}} as token_address, 
        CAST(value as double) as amount_raw
    FROM 
    {{ traces }}
    WHERE type = 'create'
    AND success 
    AND to IS NULL -- helpful filter
    AND value > UINT256 '0'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '3' Day)
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

{% if suicide_self_destruct %}
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
suicide_deposits_withdrawals
{% endif %}

{% if contract_creation_deposit %}
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
contract_creation_deposit
{% endif %}

{% endmacro %}
