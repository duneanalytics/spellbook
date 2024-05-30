{%- macro yield_yak_user_yrt_balances(
        blockchain = null
    )
-%}

{%- set future_date = '2099-12-31 00:00:00.000' -%}
{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH

{% if is_incremental() -%}
latest_balances AS (
    SELECT
        t.user_address
        , t.contract_address
        , t.from_time
        , t.yrt_balance
    FROM {{ this }} t
    WHERE
        t.to_time = TIMESTAMP '{{ future_date }}'
),
existing_contracts AS (
    SELECT
        contract_address
        , MAX(from_time) AS max_from_time
    FROM  {{ this }} t
    WHERE
        t.to_time = TIMESTAMP '{{ future_date }}'
    GROUP BY contract_address
),
{% endif -%}

new_transfers AS (
    {%- for strategy in yield_yak_strategies(blockchain) %}
        SELECT
            s.contract_address
            , s.evt_block_time AS block_time
            , s.evt_block_number AS block_number
            , u.user_address
            , SUM(u.net_transfer_amount) AS net_transfer_amount
        FROM {{ source(namespace_blockchain, strategy + '_evt_Transfer') }} s
        CROSS JOIN UNNEST(ARRAY[s."from", s.to], ARRAY[-1 * CAST(s.value AS INT256), CAST(s.value AS INT256)]) AS u(user_address, net_transfer_amount)
        {%- if is_incremental() %}
        LEFT JOIN existing_contracts c
            ON c.contract_address = s.contract_address
        WHERE
            (({{ incremental_predicate('s.evt_block_time') }}
            AND s.evt_block_time > c.max_from_time)
            OR c.contract_address IS NULL) -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
            AND s."from" != s."to"
        {%- endif %}
        {%- if not is_incremental() %}
        WHERE
            s."from" != s."to"
        {%- endif %}
        GROUP BY
            s.contract_address
            , s.evt_block_time
            , s.evt_block_number
            , u.user_address
        HAVING SUM(u.net_transfer_amount) != 0  -- Not interested in anything which results in a net transfer of 0 within a single block.
        {% if not loop.last -%}
        UNION ALL
        {%- endif -%}
    {%- endfor %}
),

combined_table AS (
    SELECT
        nt.*
    FROM new_transfers nt 
    {%- if is_incremental() %}
    -- In this we add the current balances in as if they were a single Transfer event taking place at "from_time"
    UNION ALL
    SELECT
        b.contract_address
        , b.from_time AS block_time
        , 0 AS block_number
        , b.user_address
        , b.yrt_balance AS net_transfer_amount
    FROM latest_balances b
    {%- endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , user_address
    , contract_address
    , from_time
    , to_time
    , yrt_balance
FROM (
    SELECT
        user_address
        , contract_address
        , block_time AS from_time
        , COALESCE(date_add('millisecond', -1, LEAD(block_time) OVER (PARTITION BY user_address, contract_address ORDER BY block_number)), TIMESTAMP '{{ future_date }}') AS to_time
        , SUM(net_transfer_amount) OVER (PARTITION BY user_address, contract_address ORDER BY block_number) AS yrt_balance
    FROM combined_table
)
WHERE
    to_time >= from_time -- Have this so that we aren't including rows where the balance changes within a single block or within a single millisecond.

{%- endmacro -%}