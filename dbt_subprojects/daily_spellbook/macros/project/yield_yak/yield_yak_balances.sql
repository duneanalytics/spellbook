{%- macro yield_yak_balances(
        blockchain = null
    )
-%}

{%- set future_date = '2099-12-31 00:00:00.000' -%}
{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH

{% if is_incremental() -%}
latest_balances AS (
    SELECT
        t.contract_address
        , t.from_time
        , t.deposit_token_balance
    FROM
    {{ this }} t
    WHERE
        t.to_time = TIMESTAMP '{{ future_date }}'
),
{% endif -%}

deposits_withdraws_reinvests AS (
    SELECT
        d.contract_address
        , d.evt_index
        , d.tx_index
        , d.block_time
        , d.block_number
        , d.deposit_amount
        , 0 AS withdraw_amount
        , NULL AS new_total_deposits
    FROM {{ ref(namespace_blockchain + '_deposits') }} d
    {% if is_incremental() -%}
    LEFT JOIN
    latest_balances b
        ON b.contract_address = d.contract_address
    WHERE
        ({{ incremental_predicate('d.block_time') }}
        AND d.block_time > b.from_time)
        OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
    {% endif -%}
    UNION ALL
    SELECT
        w.contract_address
        , w.evt_index
        , w.tx_index
        , w.block_time
        , w.block_number
        , 0 AS deposit_amount
        , w.withdraw_amount
        , NULL AS new_total_deposits
    FROM {{ ref(namespace_blockchain + '_withdraws') }} w
    {% if is_incremental() -%}
    LEFT JOIN
    latest_balances b
        ON b.contract_address = w.contract_address
    WHERE
        ({{ incremental_predicate('w.block_time') }}
        AND w.block_time > b.from_time)
        OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
    {% endif -%}
    UNION ALL
    SELECT
        r.contract_address
        , r.evt_index
        , r.tx_index
        , r.block_time
        , r.block_number
        , 0 AS deposit_amount
        , 0 AS withdraw_amount
        , r.new_total_deposits
    FROM {{ ref(namespace_blockchain + '_reinvests') }} r
    {% if is_incremental() -%}
    LEFT JOIN
    latest_balances b
        ON b.contract_address = r.contract_address
    WHERE
        ({{ incremental_predicate('r.block_time') }}
        AND r.block_time > b.from_time)
        OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
    {%- endif %}
),

combined_table AS (
    SELECT
        dwr.*
    FROM deposits_withdraws_reinvests dwr
    {%- if is_incremental() %}
    -- In this we add the current balances in as if they were a Reinvest event
    UNION ALL
    SELECT
        b.contract_address
        , 0 AS evt_index
        , 0 AS tx_index
        , b.from_time AS block_time
        , 0 AS block_number
        , 0 AS deposit_amount
        , 0 AS withdraw_amount
        , b.deposit_token_balance AS new_total_deposits
    FROM latest_balances b
    {%- endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , contract_address
    , from_time
    , to_time
    , deposit_token_balance
FROM (
    SELECT
        contract_address
        , block_time AS from_time
        , COALESCE(date_add('millisecond', -1, LEAD(block_time) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index)), TIMESTAMP '{{ future_date }}') AS to_time
        , CAST(SUM(COALESCE(new_total_deposits, 0)) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY block_number, tx_index, evt_index) AS INT256)
            + CAST(SUM(deposit_amount) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY block_number, tx_index, evt_index) AS INT256)
            - CAST(SUM(withdraw_amount) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY block_number, tx_index, evt_index) AS INT256) AS deposit_token_balance
    FROM (
        SELECT
            *
            , COUNT(new_total_deposits) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index) AS reinvest_partition
        FROM combined_table
    )
)
WHERE
    to_time >= from_time -- Have this so that we aren't including rows where the balance changes within a single block or within a single millisecond.

{%- endmacro -%}