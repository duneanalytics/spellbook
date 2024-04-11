{% macro yield_yak_balances(
        blockchain = null
    )
%}

{% set future_date = '2099-12-31 00:00:00.000' %}
{% set namespace_blockchain = 'yield_yak_' + blockchain %}

WITH

{% if is_incremental() %}
latest_balances AS (
    SELECT
        t.contract_address
        , t.from_time
        , t.deposit_token_balance
    FROM
    {{ this }} t
    WHERE
        t.to_block_time = TIMESTAMP '{{ future_date }}'
),
{% endif %}

deposits_withdraws_reinvests AS (
    {% for strategy in yield_yak_strategies(blockchain) %}
        SELECT
            d.contract_address
            , d.evt_block_time
            , d.evt_block_number
            , d.evt_index
            , d.amount AS deposit_amount
            , 0 AS withdraw_amount
            , NULL AS new_total_deposits
        FROM {{ source(namespace_blockchain, strategy + '_evt_Deposit') }} d
        {% if is_incremental() %}
        LEFT JOIN
        latest_balances b
            ON b.contract_address = d.contract_address
        WHERE
            ({{ incremental_predicate('d.evt_block_time') }}
            AND d.evt_block_time > b.from_block_time)
            OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
        {% endif %}
        UNION ALL
        SELECT
            w.contract_address
            , w.evt_block_time
            , w.evt_block_number
            , w.evt_index
            , 0 AS deposit_amount
            , w.amount AS withdraw_amount
            , NULL AS new_total_deposits
        FROM {{ source(namespace_blockchain, strategy + '_evt_Withdraw') }} w
        {% if is_incremental() %}
        LEFT JOIN
        latest_balances b
            ON b.contract_address = w.contract_address
        WHERE
            ({{ incremental_predicate('w.evt_block_time') }}
            AND w.evt_block_time > b.from_block_time)
            OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
        {% endif %}
        UNION ALL
        SELECT
            r.contract_address
            , r.evt_block_time
            , r.evt_block_number
            , r.evt_index
            , 0 AS deposit_amount
            , 0 AS withdraw_amount
            , r.newTotalDeposits AS new_total_deposits
        FROM {{ source(namespace_blockchain, strategy + '_evt_Reinvest') }} r
        {% if is_incremental() %}
        LEFT JOIN
        latest_balances b
            ON b.contract_address = r.contract_address
        WHERE
            ({{ incremental_predicate('r.evt_block_time') }}
            AND r.evt_block_time > b.from_block_time)
            OR b.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

combined_table AS (
    SELECT
        dwr.*
    FROM deposits_withdraws_reinvests dwr
    {% if is_incremental() %}
    -- In this we add the current balances in as if they were a Reinvest event
    UNION ALL
    SELECT
        b.contract_address
        , b.from_time AS evt_block_time
        , 0 AS evt_block_number
        , 0 AS evt_index
        , 0 AS deposit_amount
        , 0 AS withdraw_amount
        , b.deposit_token_balance AS new_total_deposits
    FROM latest_balances b
    {% endif %}
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
        , evt_block_time AS from_time
        , date_add('millisecond', -1, LEAD(evt_block_time, 1, TIMESTAMP '{{ future_date }}') OVER (PARTITION BY contract_address ORDER BY evt_block_time, evt_block_number, evt_index)) AS to_time
        , CAST(SUM(COALESCE(new_total_deposits, 0)) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY evt_block_time, evt_block_number, evt_index) AS INT256)
            + CAST(SUM(deposit_amount) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY evt_block_time, evt_block_number, evt_index) AS INT256)
            - CAST(SUM(withdraw_amount) OVER (PARTITION BY contract_address, reinvest_partition ORDER BY evt_block_time, evt_block_number, evt_index) AS INT256) AS deposit_token_balance
    FROM (
        SELECT
            *
            , COUNT(new_total_deposits) OVER (PARTITION BY contract_address ORDER BY evt_block_time, evt_block_number, evt_index) AS reinvest_partition
        FROM combined_table
    )
)
WHERE
    to_time >= from_time -- Have this so that we aren't including rows where the balance changes within a single block or within a single millisecond.

{% endmacro %}