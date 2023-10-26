{%- macro balances_aggregate_transfers(blockchain, transfers_base) %}
WITH transfers  as (
    SELECT
        blockchain,
        block_time,
        block_number,
        tx_hash,
        tx_index,
        token_standard,
        tx_from,
        tx_to,
        "to" as wallet_address,
        contract_address as token_address,
        try_cast(amount_raw as int256) as amount_raw
    FROM {{ transfers_base }}

    UNION ALL

    SELECT
        blockchain,
        block_time,
        block_number,
        tx_hash,
        tx_index,
        token_standard,
        tx_from,
        tx_to,
        "from" as wallet_address,
        contract_address as token_address,
        -try_cast(amount_raw as int256) as amount_raw
    FROM {{ transfers_base }}
)
    -- aggregate on transaction level
    SELECT
        cast(date_trunc('day', block_time) as date) as block_date,
        blockchain,
        block_time,
        block_number,
        tx_hash,
        tx_index,
        token_standard,
        tx_from,
        tx_to,
        wallet_address,
        token_address,
        sum(cast(amount_raw as double)) as amount_raw
    FROM transfers
    group by 1,2,3,4,5,6,7,8,9,10,11

{% endmacro %}