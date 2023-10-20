{%- macro balances_base(blockchain, transfers_base) %}
WITH transfers  as (
    SELECT
        'receive' as transfer_type,
        blockchain,
        block_time,
        block_number,
        tx_hash,
        tx_index,
        evt_index,
        trace_address,
        token_standard,
        tx_from,
        tx_to,
        "to" as wallet_address,
        contract_address as token_address,
        try_cast(amount_raw as int256) as amount_raw
    FROM {{ transfers_base }}

    UNION ALL

    SELECT
        'send' as transfer_type,
        blockchain,
        block_time,
        block_number,
        tx_hash,
        tx_index,
        evt_index,
        trace_address,
        token_standard,
        tx_from,
        tx_to,
        "from" as wallet_address,
        contract_address as token_address,
        -try_cast(amount_raw as int256) as amount_raw
    FROM {{ transfers_base }}
)
SELECT
    cast(date_trunc('day', block_time) as date) as block_date,
    blockchain,
    block_time,
    block_number,
    tx_hash,
    tx_index,
    evt_index,
    trace_address,
    token_standard,
    tx_from,
    tx_to,
    wallet_address,
    token_address,
    amount_raw as change_amount_raw,
    -- temporary cast as double
    try_cast(sum(cast(amount_raw as double)) over (partition by token_standard, token_address, wallet_address order by block_number, tx_index, evt_index, trace_address) as uint256) as balance_raw
FROM transfers
WHERE amount_raw is not null
{% endmacro %}