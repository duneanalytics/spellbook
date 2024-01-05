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
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}

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
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
),
{% if is_incremental() %}
existing_balances as (
    SELECT
        wallet_address,
        token_address,
        max_by(balance_raw, (block_time, tx_index)) as existing_balance_raw
    FROM {{this}}
    -- TODO: Perhaps a macro here?
    WHERE block_time < date_trunc('{{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}', now() - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}})
    GROUP BY 1,2
),
{% endif %}

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
        -- TODO: We have to temporarily cast as double because there is no way to catch overflows in sum()
        try(sum(cast(amount_raw as double))) as amount_raw
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
    -- TODO: This is related to the double casting above
    try_cast(t.amount_raw as int256) as change_amount_raw,
    {% if is_incremental() %}
    try(cast(coalesce(e.existing_balance_raw, 0) + sum(amount_raw) over (partition by t.token_standard, t.token_address, t.wallet_address order by t.block_number, t.tx_index) as uint256)) as balance_raw
    FROM aggregate_transfers t
    left join existing_balances e on e.token_address = t.token_address and e.wallet_address = t.wallet_address
    {% else %}
    try(cast(sum(amount_raw) over (partition by t.token_standard, t.token_address, t.wallet_address order by t.block_number, t.tx_index) as uint256)) as balance_raw
    FROM aggregate_transfers t
    {% endif %}

{% endmacro %}