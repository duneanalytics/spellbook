{%- macro balances_base(blockchain, transfers_base, token_standard) %}
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
    WHERE token_standard = '{{token_standard}}'

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
    WHERE token_standard = '{{token_standard}}'
),
aggregate_transfers as (
    -- aggregate on transaction level
    SELECT
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
        try(sum(amount_raw)) as amount_raw
    FROM transfers
    WHERE amount_raw is not null
    group by 1,2,3,4,5,6,7,8,9,10
)
SELECT
    cast(date_trunc('day', t.block_time) as date) as block_date,
    t.blockchain,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.tx_index,
    t.token_standard,
    t.tx_from,
    t.tx_to,
    t.wallet_address,
    t.token_address,
    t.amount_raw as change_amount_raw,
    try_cast(sum(amount_raw) over (partition by token_standard, token_address, wallet_address order by block_number, tx_index) as uint255) as balance_raw
FROM aggregate_transfers t
{% endmacro %}